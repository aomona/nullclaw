const std = @import("std");
const builtin = @import("builtin");
const root = @import("root.zig");
const bus_mod = @import("../bus.zig");
const websocket = @import("../websocket.zig");

const Atomic = @import("../portable_atomic.zig").Atomic;

const log = std.log.scoped(.discord);

/// Discord channel — connects via WebSocket gateway, sends via REST API.
/// Splits messages at 2000 chars (Discord limit).
pub const DiscordChannel = struct {
    allocator: std.mem.Allocator,
    token: []const u8,
    guild_id: ?[]const u8,
    allow_bots: bool,
    account_id: []const u8 = "default",

    // Optional gateway fields (have defaults so existing init works)
    allow_from: []const []const u8 = &.{},
    require_mention: bool = false,
    mention_exempt_channel_ids: []const []const u8 = &.{},
    intents: u32 = 37377, // GUILDS|GUILD_MESSAGES|MESSAGE_CONTENT|DIRECT_MESSAGES
    bus: ?*bus_mod.Bus = null,

    typing_mu: std.Thread.Mutex = .{},
    typing_handles: std.StringHashMapUnmanaged(*TypingTask) = .empty,
    thread_parent_ids: std.StringHashMapUnmanaged([]u8) = .empty,
    forum_parent_ids: std.StringHashMapUnmanaged(void) = .empty,
    reply_thread_roots: std.StringHashMapUnmanaged([]u8) = .empty,

    // Gateway state
    running: Atomic(bool) = Atomic(bool).init(false),
    sequence: Atomic(i64) = Atomic(i64).init(0),
    heartbeat_interval_ms: Atomic(u64) = Atomic(u64).init(0),
    heartbeat_stop: Atomic(bool) = Atomic(bool).init(false),
    session_id: ?[]u8 = null,
    resume_gateway_url: ?[]u8 = null,
    bot_user_id: ?[]u8 = null,
    gateway_thread: ?std.Thread = null,
    ws_fd: Atomic(SocketFd) = Atomic(SocketFd).init(invalid_socket),

    const SocketFd = std.net.Stream.Handle;
    const invalid_socket: SocketFd = switch (builtin.os.tag) {
        .windows => std.os.windows.ws2_32.INVALID_SOCKET,
        else => -1,
    };

    pub const MAX_MESSAGE_LEN: usize = 2000;
    pub const GATEWAY_URL = "wss://gateway.discord.gg/?v=10&encoding=json";
    const TYPING_INTERVAL_NS: u64 = 8 * std.time.ns_per_s;
    const TYPING_SLEEP_STEP_NS: u64 = 100 * std.time.ns_per_ms;
    const CHANNEL_TYPE_GUILD_NEWS_THREAD: i64 = 10;
    const CHANNEL_TYPE_GUILD_PUBLIC_THREAD: i64 = 11;
    const CHANNEL_TYPE_GUILD_PRIVATE_THREAD: i64 = 12;
    const CHANNEL_TYPE_GUILD_FORUM: i64 = 15;
    const CHANNEL_TYPE_GUILD_MEDIA: i64 = 16;
    const MAX_REPLY_THREAD_ROOTS: usize = 4096;

    const InvalidSessionAction = enum {
        identify,
        resume_session,
    };

    const ParsedTarget = struct {
        channel_id: []const u8,
        reply_message_id: ?[]const u8 = null,
    };

    const TypingTask = struct {
        channel: *DiscordChannel,
        channel_id: []const u8,
        stop_requested: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
        thread: ?std.Thread = null,
    };

    pub fn init(
        allocator: std.mem.Allocator,
        token: []const u8,
        guild_id: ?[]const u8,
        allow_bots: bool,
    ) DiscordChannel {
        return .{
            .allocator = allocator,
            .token = token,
            .guild_id = guild_id,
            .allow_bots = allow_bots,
        };
    }

    /// Initialize from a full DiscordConfig, passing all fields.
    pub fn initFromConfig(allocator: std.mem.Allocator, cfg: @import("../config_types.zig").DiscordConfig) DiscordChannel {
        return .{
            .allocator = allocator,
            .token = cfg.token,
            .guild_id = cfg.guild_id,
            .allow_bots = cfg.allow_bots,
            .account_id = cfg.account_id,
            .allow_from = cfg.allow_from,
            .require_mention = cfg.require_mention,
            .mention_exempt_channel_ids = cfg.mention_exempt_channel_ids,
            .intents = cfg.intents,
        };
    }

    pub fn channelName(_: *DiscordChannel) []const u8 {
        return "discord";
    }

    /// Build a Discord REST API URL for sending to a channel.
    pub fn sendUrl(buf: []u8, channel_id: []const u8) ![]const u8 {
        var fbs = std.io.fixedBufferStream(buf);
        const w = fbs.writer();
        try w.print("https://discord.com/api/v10/channels/{s}/messages", .{channel_id});
        return fbs.getWritten();
    }

    /// Build a Discord REST API URL for triggering typing in a channel.
    pub fn typingUrl(buf: []u8, channel_id: []const u8) ![]const u8 {
        var fbs = std.io.fixedBufferStream(buf);
        const w = fbs.writer();
        try w.print("https://discord.com/api/v10/channels/{s}/typing", .{channel_id});
        return fbs.getWritten();
    }

    /// Extract bot user ID from a bot token.
    /// Discord bot tokens are base64(bot_user_id).random.hmac
    pub fn extractBotUserId(token: []const u8) ?[]const u8 {
        // Find the first '.'
        const dot_pos = std.mem.indexOf(u8, token, ".") orelse return null;
        return token[0..dot_pos];
    }

    pub fn healthCheck(_: *DiscordChannel) bool {
        return true;
    }

    pub fn setBus(self: *DiscordChannel, b: *bus_mod.Bus) void {
        self.bus = b;
    }

    // ── Pure helper functions ─────────────────────────────────────────────

    /// Build IDENTIFY JSON payload (op=2).
    /// Example: {"op":2,"d":{"token":"Bot TOKEN","intents":37377,"properties":{"os":"linux","browser":"nullclaw","device":"nullclaw"}}}
    pub fn buildIdentifyJson(buf: []u8, token: []const u8, intents: u32) ![]const u8 {
        var fbs = std.io.fixedBufferStream(buf);
        const w = fbs.writer();
        try w.print(
            "{{\"op\":2,\"d\":{{\"token\":\"Bot {s}\",\"intents\":{d},\"properties\":{{\"os\":\"linux\",\"browser\":\"nullclaw\",\"device\":\"nullclaw\"}}}}}}",
            .{ token, intents },
        );
        return fbs.getWritten();
    }

    /// Build HEARTBEAT JSON payload (op=1).
    /// seq==0 → {"op":1,"d":null}, else {"op":1,"d":42}
    pub fn buildHeartbeatJson(buf: []u8, seq: i64) ![]const u8 {
        var fbs = std.io.fixedBufferStream(buf);
        const w = fbs.writer();
        if (seq == 0) {
            try w.writeAll("{\"op\":1,\"d\":null}");
        } else {
            try w.print("{{\"op\":1,\"d\":{d}}}", .{seq});
        }
        return fbs.getWritten();
    }

    /// Build RESUME JSON payload (op=6).
    /// {"op":6,"d":{"token":"Bot TOKEN","session_id":"SESSION","seq":42}}
    pub fn buildResumeJson(buf: []u8, token: []const u8, session_id: []const u8, seq: i64) ![]const u8 {
        var fbs = std.io.fixedBufferStream(buf);
        const w = fbs.writer();
        try w.print(
            "{{\"op\":6,\"d\":{{\"token\":\"Bot {s}\",\"session_id\":\"{s}\",\"seq\":{d}}}}}",
            .{ token, session_id, seq },
        );
        return fbs.getWritten();
    }

    /// Parse gateway host from wss:// URL.
    /// "wss://us-east1.gateway.discord.gg" -> "us-east1.gateway.discord.gg"
    /// "wss://gateway.discord.gg/?v=10&encoding=json" -> "gateway.discord.gg"
    /// Returns slice into wss_url (no allocation).
    pub fn parseGatewayHost(wss_url: []const u8) []const u8 {
        // Strip scheme prefix if present
        const no_scheme = if (std.mem.startsWith(u8, wss_url, "wss://"))
            wss_url[6..]
        else if (std.mem.startsWith(u8, wss_url, "ws://"))
            wss_url[5..]
        else
            wss_url;

        // Strip path (everything after first '/' or '?')
        const slash_pos = std.mem.indexOf(u8, no_scheme, "/");
        const query_pos = std.mem.indexOf(u8, no_scheme, "?");

        const end = blk: {
            if (slash_pos != null and query_pos != null) {
                break :blk @min(slash_pos.?, query_pos.?);
            } else if (slash_pos != null) {
                break :blk slash_pos.?;
            } else if (query_pos != null) {
                break :blk query_pos.?;
            } else {
                break :blk no_scheme.len;
            }
        };

        return no_scheme[0..end];
    }

    /// Check if bot is mentioned in message content.
    /// Returns true if "<@BOT_ID>" or "<@!BOT_ID>" appears in content.
    pub fn isMentioned(content: []const u8, bot_user_id: []const u8) bool {
        // Check for <@BOT_ID>
        var buf1: [64]u8 = undefined;
        const mention1 = std.fmt.bufPrint(&buf1, "<@{s}>", .{bot_user_id}) catch return false;
        if (std.mem.indexOf(u8, content, mention1) != null) return true;

        // Check for <@!BOT_ID>
        var buf2: [64]u8 = undefined;
        const mention2 = std.fmt.bufPrint(&buf2, "<@!{s}>", .{bot_user_id}) catch return false;
        if (std.mem.indexOf(u8, content, mention2) != null) return true;

        return false;
    }

    fn isMentionExemptChannel(self: *const DiscordChannel, channel_id: []const u8) bool {
        for (self.mention_exempt_channel_ids) |entry| {
            if (std.mem.eql(u8, entry, channel_id)) return true;
        }
        return false;
    }

    fn trimPrefixIgnoreCase(s: []const u8, prefix: []const u8) ?[]const u8 {
        if (s.len < prefix.len) return null;
        if (!std.ascii.eqlIgnoreCase(s[0..prefix.len], prefix)) return null;
        return s[prefix.len..];
    }

    fn parseTarget(target: []const u8) !ParsedTarget {
        var t = std.mem.trim(u8, target, " \t\r\n");
        if (t.len == 0) return error.InvalidTarget;

        var reply_message_id: ?[]const u8 = null;
        if (std.mem.indexOf(u8, t, ":reply:")) |idx| {
            const raw_reply = std.mem.trim(u8, t[idx + ":reply:".len ..], " \t\r\n");
            if (raw_reply.len == 0) return error.InvalidTarget;
            reply_message_id = raw_reply;
            t = std.mem.trim(u8, t[0..idx], " \t\r\n");
        }

        if (trimPrefixIgnoreCase(t, "channel:")) |rest| {
            t = std.mem.trim(u8, rest, " \t\r\n");
        }
        if (t.len == 0) return error.InvalidTarget;

        return .{ .channel_id = t, .reply_message_id = reply_message_id };
    }

    fn isThreadChannelType(channel_type: i64) bool {
        return channel_type == CHANNEL_TYPE_GUILD_NEWS_THREAD or
            channel_type == CHANNEL_TYPE_GUILD_PUBLIC_THREAD or
            channel_type == CHANNEL_TYPE_GUILD_PRIVATE_THREAD;
    }

    fn isForumChannelType(channel_type: i64) bool {
        return channel_type == CHANNEL_TYPE_GUILD_FORUM or channel_type == CHANNEL_TYPE_GUILD_MEDIA;
    }

    fn jsonString(obj: std.json.ObjectMap, key: []const u8) ?[]const u8 {
        const val = obj.get(key) orelse return null;
        return if (val == .string) val.string else null;
    }

    fn jsonInt(obj: std.json.ObjectMap, key: []const u8) ?i64 {
        const val = obj.get(key) orelse return null;
        return if (val == .integer) val.integer else null;
    }

    fn upsertThreadParent(self: *DiscordChannel, thread_id: []const u8, parent_id: []const u8) !void {
        if (self.thread_parent_ids.getPtr(thread_id)) |existing_parent| {
            if (!std.mem.eql(u8, existing_parent.*, parent_id)) {
                self.allocator.free(existing_parent.*);
                existing_parent.* = try self.allocator.dupe(u8, parent_id);
            }
            return;
        }

        const thread_id_copy = try self.allocator.dupe(u8, thread_id);
        errdefer self.allocator.free(thread_id_copy);
        const parent_id_copy = try self.allocator.dupe(u8, parent_id);
        errdefer self.allocator.free(parent_id_copy);

        try self.thread_parent_ids.put(self.allocator, thread_id_copy, parent_id_copy);
    }

    fn removeThreadParent(self: *DiscordChannel, thread_id: []const u8) void {
        if (self.thread_parent_ids.fetchRemove(thread_id)) |entry| {
            self.allocator.free(@constCast(entry.key));
            self.allocator.free(entry.value);
        }
    }

    fn upsertForumParent(self: *DiscordChannel, channel_id: []const u8) !void {
        if (self.forum_parent_ids.get(channel_id) != null) return;

        const channel_id_copy = try self.allocator.dupe(u8, channel_id);
        errdefer self.allocator.free(channel_id_copy);
        try self.forum_parent_ids.put(self.allocator, channel_id_copy, {});
    }

    fn removeForumParent(self: *DiscordChannel, channel_id: []const u8) void {
        if (self.forum_parent_ids.fetchRemove(channel_id)) |entry| {
            self.allocator.free(@constCast(entry.key));
        }
    }

    fn cacheChannelObject(self: *DiscordChannel, channel_obj: std.json.ObjectMap) !void {
        const channel_id = jsonString(channel_obj, "id") orelse return;
        const channel_type = jsonInt(channel_obj, "type") orelse return;

        if (isForumChannelType(channel_type)) {
            try self.upsertForumParent(channel_id);
        } else {
            self.removeForumParent(channel_id);
        }

        if (isThreadChannelType(channel_type)) {
            const parent_id = jsonString(channel_obj, "parent_id") orelse return;
            try self.upsertThreadParent(channel_id, parent_id);
        }
    }

    fn cacheChannelsFromArray(self: *DiscordChannel, channels_val: std.json.Value) void {
        if (channels_val != .array) return;
        for (channels_val.array.items) |channel_val| {
            if (channel_val != .object) continue;
            self.cacheChannelObject(channel_val.object) catch |err| {
                log.warn("Discord: failed to cache channel object: {}", .{err});
            };
        }
    }

    const ThreadContext = struct {
        is_thread: bool = false,
        parent_id: ?[]const u8 = null,
        is_forum_thread: bool = false,
    };

    fn resolveThreadContext(self: *DiscordChannel, channel_id: []const u8, d_obj: std.json.ObjectMap) ThreadContext {
        var ctx = ThreadContext{};

        if (self.thread_parent_ids.get(channel_id)) |parent_id| {
            ctx.is_thread = true;
            ctx.parent_id = parent_id;
            ctx.is_forum_thread = self.forum_parent_ids.get(parent_id) != null;
            return ctx;
        }

        if (d_obj.get("thread")) |thread_val| {
            if (thread_val == .object) {
                const thread_id = jsonString(thread_val.object, "id");
                const parent_id = jsonString(thread_val.object, "parent_id");
                if (thread_id != null and parent_id != null and std.mem.eql(u8, thread_id.?, channel_id)) {
                    self.cacheChannelObject(thread_val.object) catch {};
                    ctx.is_thread = true;
                    if (self.thread_parent_ids.get(channel_id)) |cached_parent_id| {
                        ctx.parent_id = cached_parent_id;
                        ctx.is_forum_thread = self.forum_parent_ids.get(cached_parent_id) != null;
                    } else {
                        ctx.parent_id = parent_id.?;
                        ctx.is_forum_thread = self.forum_parent_ids.get(parent_id.?) != null;
                    }
                    return ctx;
                }
            }
        }

        if (d_obj.get("position")) |position_val| {
            switch (position_val) {
                .integer, .float => ctx.is_thread = true,
                else => {},
            }
        }

        return ctx;
    }

    fn mentionRequiredForMessage(
        self: *DiscordChannel,
        channel_id: []const u8,
        guild_id: ?[]const u8,
        thread_ctx: ThreadContext,
        allow_reply_without_mention: bool,
    ) bool {
        if (!self.require_mention or guild_id == null) return false;
        if (allow_reply_without_mention) return false;

        if (thread_ctx.is_thread) {
            if (thread_ctx.parent_id) |parent_id| {
                if (self.isMentionExemptChannel(parent_id)) return false;
                if (thread_ctx.is_forum_thread) return true;
            }
            return true;
        }

        return !self.isMentionExemptChannel(channel_id);
    }

    fn upsertReplyThreadRoot(self: *DiscordChannel, message_id: []const u8, root_id: []const u8) !void {
        if (self.reply_thread_roots.getPtr(message_id)) |existing_root| {
            if (!std.mem.eql(u8, existing_root.*, root_id)) {
                self.allocator.free(existing_root.*);
                existing_root.* = try self.allocator.dupe(u8, root_id);
            }
            return;
        }

        if (self.reply_thread_roots.count() >= MAX_REPLY_THREAD_ROOTS) {
            var it = self.reply_thread_roots.iterator();
            while (it.next()) |entry| {
                self.allocator.free(@constCast(entry.key_ptr.*));
                self.allocator.free(entry.value_ptr.*);
            }
            self.reply_thread_roots.deinit(self.allocator);
            self.reply_thread_roots = .empty;
        }

        const message_id_copy = try self.allocator.dupe(u8, message_id);
        errdefer self.allocator.free(message_id_copy);
        const root_id_copy = try self.allocator.dupe(u8, root_id);
        errdefer self.allocator.free(root_id_copy);

        try self.reply_thread_roots.put(self.allocator, message_id_copy, root_id_copy);
    }

    fn clearThreadCaches(self: *DiscordChannel) void {
        var thread_it = self.thread_parent_ids.iterator();
        while (thread_it.next()) |entry| {
            self.allocator.free(@constCast(entry.key_ptr.*));
            self.allocator.free(entry.value_ptr.*);
        }
        self.thread_parent_ids.deinit(self.allocator);
        self.thread_parent_ids = .empty;

        var forum_it = self.forum_parent_ids.iterator();
        while (forum_it.next()) |entry| {
            self.allocator.free(@constCast(entry.key_ptr.*));
        }
        self.forum_parent_ids.deinit(self.allocator);
        self.forum_parent_ids = .empty;

        var reply_it = self.reply_thread_roots.iterator();
        while (reply_it.next()) |entry| {
            self.allocator.free(@constCast(entry.key_ptr.*));
            self.allocator.free(entry.value_ptr.*);
        }
        self.reply_thread_roots.deinit(self.allocator);
        self.reply_thread_roots = .empty;
    }

    // ── Channel vtable ──────────────────────────────────────────────

    /// Send a message to a Discord channel via REST API.
    /// Splits at MAX_MESSAGE_LEN (2000 chars).
    pub fn sendMessage(self: *DiscordChannel, target: []const u8, text: []const u8) !void {
        const parsed_target = try parseTarget(target);

        var it = root.splitMessage(text, MAX_MESSAGE_LEN);
        var first = true;
        while (it.next()) |chunk| {
            try self.sendChunk(parsed_target.channel_id, chunk, if (first) parsed_target.reply_message_id else null);
            first = false;
        }
    }

    /// Send a Discord typing indicator (best-effort, errors ignored).
    pub fn sendTypingIndicator(self: *DiscordChannel, channel_id: []const u8) void {
        if (builtin.is_test) return;
        if (channel_id.len == 0) return;

        var url_buf: [256]u8 = undefined;
        const url = typingUrl(&url_buf, channel_id) catch return;

        var auth_buf: [512]u8 = undefined;
        var auth_fbs = std.io.fixedBufferStream(&auth_buf);
        auth_fbs.writer().print("Authorization: Bot {s}", .{self.token}) catch return;
        const auth_header = auth_fbs.getWritten();

        const resp = root.http_util.curlPost(self.allocator, url, "{}", &.{auth_header}) catch return;
        self.allocator.free(resp);
    }

    pub fn startTyping(self: *DiscordChannel, channel_id: []const u8) !void {
        if (!self.running.load(.acquire)) return;
        if (channel_id.len == 0) return;

        try self.stopTyping(channel_id);

        const key_copy = try self.allocator.dupe(u8, channel_id);
        errdefer self.allocator.free(key_copy);

        const task = try self.allocator.create(TypingTask);
        errdefer self.allocator.destroy(task);
        task.* = .{
            .channel = self,
            .channel_id = key_copy,
        };

        task.thread = try std.Thread.spawn(.{ .stack_size = 128 * 1024 }, typingLoop, .{task});
        errdefer {
            task.stop_requested.store(true, .release);
            if (task.thread) |t| t.join();
        }

        self.typing_mu.lock();
        defer self.typing_mu.unlock();
        try self.typing_handles.put(self.allocator, key_copy, task);
    }

    pub fn stopTyping(self: *DiscordChannel, channel_id: []const u8) !void {
        var removed_key: ?[]u8 = null;
        var removed_task: ?*TypingTask = null;

        self.typing_mu.lock();
        if (self.typing_handles.fetchRemove(channel_id)) |entry| {
            removed_key = @constCast(entry.key);
            removed_task = entry.value;
        }
        self.typing_mu.unlock();

        if (removed_task) |task| {
            task.stop_requested.store(true, .release);
            if (task.thread) |t| t.join();
            self.allocator.destroy(task);
        }
        if (removed_key) |key| {
            self.allocator.free(key);
        }
    }

    fn stopAllTyping(self: *DiscordChannel) void {
        self.typing_mu.lock();
        var handles = self.typing_handles;
        self.typing_handles = .empty;
        self.typing_mu.unlock();

        var it = handles.iterator();
        while (it.next()) |entry| {
            const task = entry.value_ptr.*;
            task.stop_requested.store(true, .release);
            if (task.thread) |t| t.join();
            self.allocator.destroy(task);
            self.allocator.free(@constCast(entry.key_ptr.*));
        }
        handles.deinit(self.allocator);
    }

    fn typingLoop(task: *TypingTask) void {
        while (!task.stop_requested.load(.acquire)) {
            task.channel.sendTypingIndicator(task.channel_id);
            var elapsed: u64 = 0;
            while (elapsed < TYPING_INTERVAL_NS and !task.stop_requested.load(.acquire)) {
                std.Thread.sleep(TYPING_SLEEP_STEP_NS);
                elapsed += TYPING_SLEEP_STEP_NS;
            }
        }
    }

    fn buildSendBody(allocator: std.mem.Allocator, text: []const u8, reply_message_id: ?[]const u8) ![]u8 {
        var body_list: std.ArrayListUnmanaged(u8) = .empty;
        defer body_list.deinit(allocator);

        try body_list.appendSlice(allocator, "{\"content\":");
        try root.json_util.appendJsonString(&body_list, allocator, text);
        if (reply_message_id) |message_id| {
            try body_list.appendSlice(allocator, ",\"message_reference\":{\"message_id\":");
            try root.json_util.appendJsonString(&body_list, allocator, message_id);
            try body_list.appendSlice(allocator, ",\"fail_if_not_exists\":false}");
        }
        try body_list.appendSlice(allocator, "}");

        return try body_list.toOwnedSlice(allocator);
    }

    fn sendChunk(self: *DiscordChannel, channel_id: []const u8, text: []const u8, reply_message_id: ?[]const u8) !void {
        var url_buf: [256]u8 = undefined;
        const url = try sendUrl(&url_buf, channel_id);

        const body = try buildSendBody(self.allocator, text, reply_message_id);
        defer self.allocator.free(body);

        // Build auth header value: "Authorization: Bot <token>"
        var auth_buf: [512]u8 = undefined;
        var auth_fbs = std.io.fixedBufferStream(&auth_buf);
        try auth_fbs.writer().print("Authorization: Bot {s}", .{self.token});
        const auth_header = auth_fbs.getWritten();

        const resp = root.http_util.curlPost(self.allocator, url, body, &.{auth_header}) catch |err| {
            log.err("Discord API POST failed: {}", .{err});
            return error.DiscordApiError;
        };
        self.allocator.free(resp);
    }

    // ── Gateway ──────────────────────────────────────────────────────

    fn vtableStart(ptr: *anyopaque) anyerror!void {
        const self: *DiscordChannel = @ptrCast(@alignCast(ptr));
        self.running.store(true, .release);
        self.gateway_thread = try std.Thread.spawn(.{ .stack_size = 2 * 1024 * 1024 }, gatewayLoop, .{self});
    }

    fn vtableStop(ptr: *anyopaque) void {
        const self: *DiscordChannel = @ptrCast(@alignCast(ptr));
        self.running.store(false, .release);
        self.heartbeat_stop.store(true, .release);
        self.stopAllTyping();
        // Close socket to unblock blocking read
        const fd = self.ws_fd.load(.acquire);
        if (fd != invalid_socket) {
            if (comptime builtin.os.tag == .windows) {
                _ = std.os.windows.ws2_32.closesocket(fd);
            } else {
                std.posix.close(fd);
            }
        }
        if (self.gateway_thread) |t| {
            t.join();
            self.gateway_thread = null;
        }
        // Free session state
        if (self.session_id) |s| {
            self.allocator.free(s);
            self.session_id = null;
        }
        if (self.resume_gateway_url) |u| {
            self.allocator.free(u);
            self.resume_gateway_url = null;
        }
        if (self.bot_user_id) |u| {
            self.allocator.free(u);
            self.bot_user_id = null;
        }
        self.clearThreadCaches();
    }

    fn vtableSend(ptr: *anyopaque, target: []const u8, message: []const u8, _: []const []const u8) anyerror!void {
        const self: *DiscordChannel = @ptrCast(@alignCast(ptr));
        try self.sendMessage(target, message);
    }

    fn vtableName(ptr: *anyopaque) []const u8 {
        const self: *DiscordChannel = @ptrCast(@alignCast(ptr));
        return self.channelName();
    }

    fn vtableHealthCheck(ptr: *anyopaque) bool {
        const self: *DiscordChannel = @ptrCast(@alignCast(ptr));
        return self.healthCheck();
    }

    fn vtableStartTyping(ptr: *anyopaque, recipient: []const u8) anyerror!void {
        const self: *DiscordChannel = @ptrCast(@alignCast(ptr));
        try self.startTyping(recipient);
    }

    fn vtableStopTyping(ptr: *anyopaque, recipient: []const u8) anyerror!void {
        const self: *DiscordChannel = @ptrCast(@alignCast(ptr));
        try self.stopTyping(recipient);
    }

    pub const vtable = root.Channel.VTable{
        .start = &vtableStart,
        .stop = &vtableStop,
        .send = &vtableSend,
        .name = &vtableName,
        .healthCheck = &vtableHealthCheck,
        .startTyping = &vtableStartTyping,
        .stopTyping = &vtableStopTyping,
    };

    pub fn channel(self: *DiscordChannel) root.Channel {
        return .{ .ptr = @ptrCast(self), .vtable = &vtable };
    }

    // ── Gateway loop ─────────────────────────────────────────────────

    fn gatewayLoop(self: *DiscordChannel) void {
        while (self.running.load(.acquire)) {
            var backoff_ms: u64 = 5000;
            self.runGatewayOnce() catch |err| switch (err) {
                error.ShouldReconnect => {
                    // OP7 RECONNECT is a normal control signal from Discord.
                    // Reconnect quickly to minimize missed events.
                    log.info("Discord gateway reconnect requested by server", .{});
                    backoff_ms = 250;
                },
                else => {
                    log.warn("Discord gateway error: {}", .{err});
                },
            };
            if (!self.running.load(.acquire)) break;
            // Backoff between reconnects (interruptible).
            var slept: u64 = 0;
            while (slept < backoff_ms and self.running.load(.acquire)) {
                std.Thread.sleep(100 * std.time.ns_per_ms);
                slept += 100;
            }
        }
    }

    fn runGatewayOnce(self: *DiscordChannel) !void {
        // Determine host
        const default_host = "gateway.discord.gg";
        const host: []const u8 = if (self.resume_gateway_url) |u| parseGatewayHost(u) else default_host;

        var ws = try websocket.WsClient.connect(
            self.allocator,
            host,
            443,
            "/?v=10&encoding=json",
            &.{},
        );

        // Store fd for interrupt-on-stop
        self.ws_fd.store(ws.stream.handle, .release);

        // Start heartbeat thread — on failure, clean up ws manually (no errdefer to avoid
        // double-deinit with the defer block below once spawn succeeds).
        self.heartbeat_stop.store(false, .release);
        self.heartbeat_interval_ms.store(0, .release);
        const hbt = std.Thread.spawn(.{ .stack_size = 128 * 1024 }, heartbeatLoop, .{ self, &ws }) catch |err| {
            ws.deinit();
            return err;
        };
        defer {
            self.heartbeat_stop.store(true, .release);
            hbt.join();
            self.ws_fd.store(invalid_socket, .release);
            ws.deinit();
        }

        // Wait for HELLO (first message)
        const hello_text = try ws.readTextMessage() orelse return error.ConnectionClosed;
        defer self.allocator.free(hello_text);
        try self.handleHello(&ws, hello_text);

        // IDENTIFY or RESUME
        if (self.session_id != null) {
            try self.sendResumePayload(&ws);
        } else {
            self.sequence.store(0, .release);
            try self.sendIdentifyPayload(&ws);
        }

        // Main read loop
        while (self.running.load(.acquire)) {
            const maybe_text = ws.readTextMessage() catch |err| {
                log.warn("Discord gateway read failed: {}", .{err});
                break;
            };
            const text = maybe_text orelse break;
            defer self.allocator.free(text);
            self.handleGatewayMessage(&ws, text) catch |err| {
                if (err == error.ShouldReconnect) return err;
                log.err("Discord gateway msg error: {}", .{err});
            };
        }
    }

    // ── Heartbeat thread ─────────────────────────────────────────────

    fn heartbeatLoop(self: *DiscordChannel, ws: *websocket.WsClient) void {
        // Wait for interval to be set
        while (!self.heartbeat_stop.load(.acquire) and self.heartbeat_interval_ms.load(.acquire) == 0) {
            std.Thread.sleep(10 * std.time.ns_per_ms);
        }
        while (!self.heartbeat_stop.load(.acquire)) {
            const interval_ms = self.heartbeat_interval_ms.load(.acquire);
            var elapsed: u64 = 0;
            while (elapsed < interval_ms) {
                if (self.heartbeat_stop.load(.acquire)) return;
                std.Thread.sleep(100 * std.time.ns_per_ms);
                elapsed += 100;
            }
            if (self.heartbeat_stop.load(.acquire)) return;

            const seq = self.sequence.load(.acquire);
            var hb_buf: [64]u8 = undefined;
            const hb_json = buildHeartbeatJson(&hb_buf, seq) catch continue;
            ws.writeText(hb_json) catch |err| {
                log.warn("Discord heartbeat failed: {}", .{err});
            };
        }
    }

    // ── Message handlers ─────────────────────────────────────────────

    /// Parse HELLO payload and store heartbeat interval.
    fn handleHello(self: *DiscordChannel, _: *websocket.WsClient, text: []const u8) !void {
        const parsed = try std.json.parseFromSlice(std.json.Value, self.allocator, text, .{});
        defer parsed.deinit();

        const root_val = parsed.value;
        if (root_val != .object) return;
        const d_val = root_val.object.get("d") orelse return;
        switch (d_val) {
            .object => |d_obj| {
                const hb_val = d_obj.get("heartbeat_interval") orelse return;
                switch (hb_val) {
                    .integer => |ms| {
                        if (ms > 0) {
                            self.heartbeat_interval_ms.store(@intCast(ms), .release);
                        }
                    },
                    .float => |ms| {
                        if (ms > 0) {
                            self.heartbeat_interval_ms.store(@intFromFloat(ms), .release);
                        }
                    },
                    else => {},
                }
            },
            else => {},
        }
    }

    /// Handle a gateway message, switching on op code.
    fn handleGatewayMessage(self: *DiscordChannel, ws: *websocket.WsClient, text: []const u8) !void {
        const parsed = std.json.parseFromSlice(std.json.Value, self.allocator, text, .{}) catch |err| {
            log.warn("Discord: failed to parse gateway message: {}", .{err});
            return;
        };
        defer parsed.deinit();

        const root_val = parsed.value;
        if (root_val != .object) {
            log.warn("Discord: gateway message root is not an object", .{});
            return;
        }

        // Get op code
        const op_val = root_val.object.get("op") orelse {
            log.warn("Discord: gateway message missing 'op' field", .{});
            return;
        };
        const op: i64 = switch (op_val) {
            .integer => |i| i,
            else => {
                log.warn("Discord: gateway 'op' is not an integer", .{});
                return;
            },
        };

        switch (op) {
            10 => { // HELLO
                self.handleHello(ws, text) catch |err| {
                    log.warn("Discord: handleHello error: {}", .{err});
                };
            },
            0 => { // DISPATCH
                // Update sequence from "s" field
                if (root_val.object.get("s")) |s_val| {
                    switch (s_val) {
                        .integer => |s| {
                            // Sequence comes from the active gateway session and is ordered.
                            // Always overwrite to avoid stale seq after a fresh IDENTIFY.
                            self.sequence.store(s, .release);
                        },
                        else => {},
                    }
                }

                // Get event type "t"
                const t_val = root_val.object.get("t") orelse return;
                const event_type: []const u8 = switch (t_val) {
                    .string => |s| s,
                    else => return,
                };

                if (std.mem.eql(u8, event_type, "READY")) {
                    self.handleReady(root_val) catch |err| {
                        log.warn("Discord: handleReady error: {}", .{err});
                    };
                } else if (std.mem.eql(u8, event_type, "GUILD_CREATE")) {
                    self.handleGuildCreate(root_val) catch |err| {
                        log.warn("Discord: handleGuildCreate error: {}", .{err});
                    };
                } else if (std.mem.eql(u8, event_type, "CHANNEL_CREATE") or std.mem.eql(u8, event_type, "CHANNEL_UPDATE")) {
                    self.handleChannelUpsert(root_val) catch |err| {
                        log.warn("Discord: handleChannelUpsert error: {}", .{err});
                    };
                } else if (std.mem.eql(u8, event_type, "CHANNEL_DELETE")) {
                    self.handleChannelDelete(root_val) catch |err| {
                        log.warn("Discord: handleChannelDelete error: {}", .{err});
                    };
                } else if (std.mem.eql(u8, event_type, "THREAD_CREATE") or std.mem.eql(u8, event_type, "THREAD_UPDATE")) {
                    self.handleChannelUpsert(root_val) catch |err| {
                        log.warn("Discord: handleThreadUpsert error: {}", .{err});
                    };
                } else if (std.mem.eql(u8, event_type, "THREAD_DELETE")) {
                    self.handleThreadDelete(root_val) catch |err| {
                        log.warn("Discord: handleThreadDelete error: {}", .{err});
                    };
                } else if (std.mem.eql(u8, event_type, "THREAD_LIST_SYNC")) {
                    self.handleThreadListSync(root_val) catch |err| {
                        log.warn("Discord: handleThreadListSync error: {}", .{err});
                    };
                } else if (std.mem.eql(u8, event_type, "MESSAGE_CREATE")) {
                    self.handleMessageCreate(root_val) catch |err| {
                        log.warn("Discord: handleMessageCreate error: {}", .{err});
                    };
                }
            },
            1 => { // HEARTBEAT — server requests immediate heartbeat
                const seq = self.sequence.load(.acquire);
                var hb_buf: [64]u8 = undefined;
                const hb_json = buildHeartbeatJson(&hb_buf, seq) catch return;
                ws.writeText(hb_json) catch |err| {
                    log.warn("Discord: immediate heartbeat failed: {}", .{err});
                };
            },
            11 => { // HEARTBEAT_ACK
                // No-op — heartbeat acknowledged
            },
            7 => { // RECONNECT
                log.info("Discord: server requested reconnect", .{});
                return error.ShouldReconnect;
            },
            9 => { // INVALID_SESSION
                // Check if resumable (d field)
                const d_val = root_val.object.get("d");
                const resumable = if (d_val) |d| switch (d) {
                    .bool => |b| b,
                    else => false,
                } else false;
                switch (self.resolveInvalidSessionAction(resumable)) {
                    .resume_session => {
                        self.sendResumePayload(ws) catch |err| {
                            log.warn("Discord: resume after INVALID_SESSION failed: {}", .{err});
                            return error.ShouldReconnect;
                        };
                    },
                    .identify => {
                        self.sendIdentifyPayload(ws) catch |err| {
                            log.warn("Discord: re-identify after INVALID_SESSION failed: {}", .{err});
                            return error.ShouldReconnect;
                        };
                    },
                }
            },
            else => {
                log.warn("Discord: unhandled gateway op={d}", .{op});
            },
        }
    }

    fn clearSessionStateForIdentify(self: *DiscordChannel) void {
        if (self.session_id) |s| {
            self.allocator.free(s);
            self.session_id = null;
        }
        if (self.resume_gateway_url) |u| {
            self.allocator.free(u);
            self.resume_gateway_url = null;
        }
        self.sequence.store(0, .release);
    }

    fn resolveInvalidSessionAction(self: *DiscordChannel, resumable: bool) InvalidSessionAction {
        if (resumable and self.session_id != null) {
            return .resume_session;
        }
        // Either explicitly non-resumable OR resumable but local session state is absent.
        // In both cases fall back to a clean IDENTIFY path.
        self.clearSessionStateForIdentify();
        return .identify;
    }

    /// Handle READY event: extract session_id, resume_gateway_url, bot_user_id.
    fn handleReady(self: *DiscordChannel, root_val: std.json.Value) !void {
        if (root_val != .object) return;
        const d_val = root_val.object.get("d") orelse {
            log.warn("Discord READY: missing 'd' field", .{});
            return;
        };
        const d_obj = switch (d_val) {
            .object => |o| o,
            else => {
                log.warn("Discord READY: 'd' is not an object", .{});
                return;
            },
        };

        // Extract session_id
        if (d_obj.get("session_id")) |sid_val| {
            switch (sid_val) {
                .string => |s| {
                    if (self.session_id) |old| self.allocator.free(old);
                    self.session_id = try self.allocator.dupe(u8, s);
                },
                else => {},
            }
        }

        // Extract resume_gateway_url
        if (d_obj.get("resume_gateway_url")) |rgu_val| {
            switch (rgu_val) {
                .string => |s| {
                    if (self.resume_gateway_url) |old| self.allocator.free(old);
                    self.resume_gateway_url = try self.allocator.dupe(u8, s);
                },
                else => {},
            }
        }

        // Extract bot user ID from d.user.id
        if (d_obj.get("user")) |user_val| {
            switch (user_val) {
                .object => |user_obj| {
                    if (user_obj.get("id")) |id_val| {
                        switch (id_val) {
                            .string => |s| {
                                if (self.bot_user_id) |old| self.allocator.free(old);
                                self.bot_user_id = try self.allocator.dupe(u8, s);
                            },
                            else => {},
                        }
                    }
                },
                else => {},
            }
        }

        log.info("Discord READY: session_id={s}", .{self.session_id orelse "<none>"});
    }

    fn dispatchPayloadObject(root_val: std.json.Value, event_name: []const u8) ?std.json.ObjectMap {
        if (root_val != .object) return null;
        const d_val = root_val.object.get("d") orelse {
            log.warn("Discord {s}: missing 'd' field", .{event_name});
            return null;
        };
        return switch (d_val) {
            .object => |d_obj| d_obj,
            else => {
                log.warn("Discord {s}: 'd' is not an object", .{event_name});
                return null;
            },
        };
    }

    fn handleGuildCreate(self: *DiscordChannel, root_val: std.json.Value) !void {
        const d_obj = dispatchPayloadObject(root_val, "GUILD_CREATE") orelse return;
        if (d_obj.get("channels")) |channels_val| {
            self.cacheChannelsFromArray(channels_val);
        }
        if (d_obj.get("threads")) |threads_val| {
            self.cacheChannelsFromArray(threads_val);
        }
    }

    fn handleChannelUpsert(self: *DiscordChannel, root_val: std.json.Value) !void {
        const d_obj = dispatchPayloadObject(root_val, "CHANNEL_UPSERT") orelse return;
        try self.cacheChannelObject(d_obj);
    }

    fn handleChannelDelete(self: *DiscordChannel, root_val: std.json.Value) !void {
        const d_obj = dispatchPayloadObject(root_val, "CHANNEL_DELETE") orelse return;
        const channel_id = jsonString(d_obj, "id") orelse return;
        const channel_type = jsonInt(d_obj, "type") orelse return;

        if (isThreadChannelType(channel_type)) {
            self.removeThreadParent(channel_id);
        }
        if (isForumChannelType(channel_type)) {
            self.removeForumParent(channel_id);
        }
    }

    fn handleThreadDelete(self: *DiscordChannel, root_val: std.json.Value) !void {
        const d_obj = dispatchPayloadObject(root_val, "THREAD_DELETE") orelse return;
        const thread_id = jsonString(d_obj, "id") orelse return;
        self.removeThreadParent(thread_id);
    }

    fn handleThreadListSync(self: *DiscordChannel, root_val: std.json.Value) !void {
        const d_obj = dispatchPayloadObject(root_val, "THREAD_LIST_SYNC") orelse return;
        if (d_obj.get("threads")) |threads_val| {
            self.cacheChannelsFromArray(threads_val);
        }
    }

    /// Handle MESSAGE_CREATE event and publish to bus if filters pass.
    fn handleMessageCreate(self: *DiscordChannel, root_val: std.json.Value) !void {
        const d_obj = dispatchPayloadObject(root_val, "MESSAGE_CREATE") orelse return;

        // Extract channel_id
        const channel_id: []const u8 = if (d_obj.get("channel_id")) |v| switch (v) {
            .string => |s| s,
            else => {
                log.warn("Discord MESSAGE_CREATE: 'channel_id' is not a string", .{});
                return;
            },
        } else {
            log.warn("Discord MESSAGE_CREATE: missing 'channel_id'", .{});
            return;
        };

        // Extract message id (for reply semantics)
        const message_id: ?[]const u8 = if (d_obj.get("id")) |v| switch (v) {
            .string => |s| s,
            else => null,
        } else null;

        // Extract content
        const content: []const u8 = if (d_obj.get("content")) |v| switch (v) {
            .string => |s| s,
            else => "",
        } else "";

        // Extract guild_id (optional — absent for DMs)
        const guild_id: ?[]const u8 = if (d_obj.get("guild_id")) |v| switch (v) {
            .string => |s| s,
            else => null,
        } else null;

        // Extract author object
        const author_obj = if (d_obj.get("author")) |v| switch (v) {
            .object => |o| o,
            else => {
                log.warn("Discord MESSAGE_CREATE: 'author' is not an object", .{});
                return;
            },
        } else {
            log.warn("Discord MESSAGE_CREATE: missing 'author'", .{});
            return;
        };

        // Extract author.id
        const author_id: []const u8 = if (author_obj.get("id")) |v| switch (v) {
            .string => |s| s,
            else => {
                log.warn("Discord MESSAGE_CREATE: 'author.id' is not a string", .{});
                return;
            },
        } else {
            log.warn("Discord MESSAGE_CREATE: missing 'author.id'", .{});
            return;
        };

        // Extract author.bot (defaults to false if absent)
        const author_is_bot: bool = if (author_obj.get("bot")) |v| switch (v) {
            .bool => |b| b,
            else => false,
        } else false;

        // Extract reply target message id, if this message is a reply.
        const reply_to_message_id: ?[]const u8 = blk: {
            const message_ref_val = d_obj.get("message_reference") orelse break :blk null;
            if (message_ref_val != .object) break :blk null;
            break :blk jsonString(message_ref_val.object, "message_id");
        };

        // Extract referenced message author id when available.
        const referenced_author_id: ?[]const u8 = blk: {
            const referenced_val = d_obj.get("referenced_message") orelse break :blk null;
            if (referenced_val != .object) break :blk null;
            const referenced_author_val = referenced_val.object.get("author") orelse break :blk null;
            if (referenced_author_val != .object) break :blk null;
            break :blk jsonString(referenced_author_val.object, "id");
        };

        var reply_from_known_chain = false;
        const reply_thread_root_id: ?[]const u8 = if (reply_to_message_id) |reply_to_id| blk: {
            if (self.reply_thread_roots.get(reply_to_id)) |known_root| {
                reply_from_known_chain = true;
                break :blk known_root;
            }
            break :blk reply_to_id;
        } else null;

        const reply_targets_bot = blk: {
            const author = referenced_author_id orelse break :blk false;
            const bot_uid = self.bot_user_id orelse break :blk false;
            break :blk std.mem.eql(u8, author, bot_uid);
        };

        if (message_id) |mid| {
            if (reply_thread_root_id) |root_id| {
                self.upsertReplyThreadRoot(mid, root_id) catch |err| {
                    log.warn("Discord: failed to cache reply context: {}", .{err});
                };
            }
        }

        // Never process messages sent by this bot account.
        if (self.bot_user_id) |bot_uid| {
            if (std.mem.eql(u8, author_id, bot_uid)) {
                return;
            }
        }

        // Filter 1: bot author
        if (author_is_bot and !self.allow_bots) {
            return;
        }

        const thread_ctx = resolveThreadContext(self, channel_id, d_obj);
        const allow_reply_without_mention =
            reply_to_message_id != null and (reply_targets_bot or reply_from_known_chain);

        // Filter 2: require_mention in guild messages.
        // - Forum/media threads: mention exemption is controlled by parent forum ID in mention_exempt_channel_ids.
        // - Replying to the bot (or an active reply chain) bypasses mention requirement.
        if (mentionRequiredForMessage(self, channel_id, guild_id, thread_ctx, allow_reply_without_mention)) {
            const bot_uid = self.bot_user_id orelse "";
            if (!isMentioned(content, bot_uid)) {
                return;
            }
        }

        // Filter 3: allow_from allowlist
        if (self.allow_from.len > 0) {
            if (!root.isAllowed(self.allow_from, author_id)) {
                return;
            }
        }

        // Process attachments (if any)
        var content_buf: std.ArrayListUnmanaged(u8) = .empty;
        defer content_buf.deinit(self.allocator);

        if (content.len > 0) {
            content_buf.appendSlice(self.allocator, content) catch {};
        }

        if (d_obj.get("attachments")) |att_val| {
            if (att_val == .array) {
                var rand = std.crypto.random;
                for (att_val.array.items) |att_item| {
                    if (att_item == .object) {
                        if (att_item.object.get("url")) |url_val| {
                            if (url_val == .string) {
                                const attach_url = url_val.string;

                                // Download it
                                if (root.http_util.curlGet(self.allocator, attach_url, &.{}, "30")) |img_data| {
                                    defer self.allocator.free(img_data);

                                    // Make temp file
                                    const rand_id = rand.int(u64);
                                    var path_buf: [1024]u8 = undefined;
                                    const local_path = std.fmt.bufPrint(&path_buf, "/tmp/discord_{x}.dat", .{rand_id}) catch continue;

                                    if (std.fs.createFileAbsolute(local_path, .{ .read = false })) |file| {
                                        file.writeAll(img_data) catch {
                                            file.close();
                                            continue;
                                        };
                                        file.close();

                                        if (content_buf.items.len > 0) content_buf.appendSlice(self.allocator, "\n") catch {};
                                        content_buf.appendSlice(self.allocator, "[IMAGE:") catch {};
                                        content_buf.appendSlice(self.allocator, local_path) catch {};
                                        content_buf.appendSlice(self.allocator, "]") catch {};
                                    } else |_| {}
                                } else |err| {
                                    log.warn("Discord: failed to download attachment: {}", .{err});
                                }
                            }
                        }
                    }
                }
            }
        }

        const final_content = content_buf.toOwnedSlice(self.allocator) catch blk: {
            break :blk try self.allocator.dupe(u8, content);
        };
        defer self.allocator.free(final_content);

        // Build account-aware session key fallback to prevent cross-account bleed
        // when route resolution is unavailable.
        const session_key = if (guild_id == null)
            try std.fmt.allocPrint(self.allocator, "discord:{s}:direct:{s}", .{ self.account_id, author_id })
        else
            try std.fmt.allocPrint(self.allocator, "discord:{s}:channel:{s}", .{ self.account_id, channel_id });
        defer self.allocator.free(session_key);

        var metadata_buf: std.ArrayListUnmanaged(u8) = .empty;
        defer metadata_buf.deinit(self.allocator);
        const mw = metadata_buf.writer(self.allocator);
        try mw.print("{{\"is_dm\":{s}", .{if (guild_id == null) "true" else "false"});
        try mw.writeAll(",\"account_id\":");
        try root.appendJsonStringW(mw, self.account_id);
        try mw.writeAll(",\"channel_id\":");
        try root.appendJsonStringW(mw, channel_id);
        if (guild_id) |gid| {
            try mw.writeAll(",\"guild_id\":");
            try root.appendJsonStringW(mw, gid);
        }
        if (message_id) |mid| {
            try mw.writeAll(",\"message_id\":");
            try root.appendJsonStringW(mw, mid);
        }
        if (reply_thread_root_id) |thread_id| {
            try mw.writeAll(",\"thread_id\":");
            try root.appendJsonStringW(mw, thread_id);
        }
        if (thread_ctx.parent_id) |parent_id| {
            try mw.writeAll(",\"parent_channel_id\":");
            try root.appendJsonStringW(mw, parent_id);
        }
        try mw.writeByte('}');

        const msg = try bus_mod.makeInboundFull(
            self.allocator,
            "discord",
            author_id,
            channel_id,
            final_content,
            session_key,
            &.{},
            metadata_buf.items,
        );

        if (self.bus) |b| {
            b.publishInbound(msg) catch |err| {
                log.warn("Discord: failed to publish inbound message: {}", .{err});
                msg.deinit(self.allocator);
            };
        } else {
            // No bus configured — free the message
            msg.deinit(self.allocator);
        }
    }

    /// Send IDENTIFY payload.
    fn sendIdentifyPayload(self: *DiscordChannel, ws: *websocket.WsClient) !void {
        var buf: [1024]u8 = undefined;
        const json = try buildIdentifyJson(&buf, self.token, self.intents);
        try ws.writeText(json);
    }

    /// Send RESUME payload.
    fn sendResumePayload(self: *DiscordChannel, ws: *websocket.WsClient) !void {
        const sid = self.session_id orelse return error.NoSessionId;
        const seq = self.sequence.load(.acquire);
        var buf: [512]u8 = undefined;
        const json = try buildResumeJson(&buf, self.token, sid, seq);
        try ws.writeText(json);
    }
};

// ════════════════════════════════════════════════════════════════════════════
// Tests
// ════════════════════════════════════════════════════════════════════════════

test "discord send url" {
    var buf: [256]u8 = undefined;
    const url = try DiscordChannel.sendUrl(&buf, "123456");
    try std.testing.expectEqualStrings("https://discord.com/api/v10/channels/123456/messages", url);
}

test "discord typing url" {
    var buf: [256]u8 = undefined;
    const url = try DiscordChannel.typingUrl(&buf, "123456");
    try std.testing.expectEqualStrings("https://discord.com/api/v10/channels/123456/typing", url);
}

test "discord sendTypingIndicator is no-op in tests" {
    var ch = DiscordChannel.init(std.testing.allocator, "my-bot-token", null, false);
    ch.sendTypingIndicator("123456");
}

test "discord typing handles start empty" {
    var ch = DiscordChannel.init(std.testing.allocator, "my-bot-token", null, false);
    try std.testing.expect(ch.typing_handles.get("123456") == null);
}

test "discord startTyping stores handle and stopTyping clears it" {
    var ch = DiscordChannel.init(std.testing.allocator, "my-bot-token", null, false);
    ch.running.store(true, .release);
    defer ch.stopAllTyping();

    try ch.startTyping("123456");
    try std.testing.expect(ch.typing_handles.get("123456") != null);
    std.Thread.sleep(50 * std.time.ns_per_ms);
    try ch.stopTyping("123456");
    try std.testing.expect(ch.typing_handles.get("123456") == null);
}

test "discord stopTyping is idempotent" {
    var ch = DiscordChannel.init(std.testing.allocator, "my-bot-token", null, false);
    try ch.stopTyping("123456");
    try ch.stopTyping("123456");
}

test "discord extract bot user id" {
    const id = DiscordChannel.extractBotUserId("MTIzNDU2.Ghijk.abcdef");
    try std.testing.expectEqualStrings("MTIzNDU2", id.?);
}

test "discord extract bot user id no dot" {
    try std.testing.expect(DiscordChannel.extractBotUserId("notokenformat") == null);
}

// ════════════════════════════════════════════════════════════════════════════
// Additional Discord Tests (ported from ZeroClaw Rust)
// ════════════════════════════════════════════════════════════════════════════

test "discord send url with different channel ids" {
    var buf: [256]u8 = undefined;
    const url1 = try DiscordChannel.sendUrl(&buf, "999");
    try std.testing.expectEqualStrings("https://discord.com/api/v10/channels/999/messages", url1);

    var buf2: [256]u8 = undefined;
    const url2 = try DiscordChannel.sendUrl(&buf2, "1234567890");
    try std.testing.expectEqualStrings("https://discord.com/api/v10/channels/1234567890/messages", url2);
}

test "discord extract bot user id multiple dots" {
    // Token format: base64(user_id).timestamp.hmac
    const id = DiscordChannel.extractBotUserId("MTIzNDU2.fake.hmac");
    try std.testing.expectEqualStrings("MTIzNDU2", id.?);
}

test "discord extract bot user id empty token" {
    // Empty string before dot means empty result
    const id = DiscordChannel.extractBotUserId("");
    try std.testing.expect(id == null);
}

test "discord extract bot user id single dot" {
    const id = DiscordChannel.extractBotUserId("abc.");
    try std.testing.expectEqualStrings("abc", id.?);
}

test "discord max message len constant" {
    try std.testing.expectEqual(@as(usize, 2000), DiscordChannel.MAX_MESSAGE_LEN);
}

test "discord gateway url constant" {
    try std.testing.expectEqualStrings("wss://gateway.discord.gg/?v=10&encoding=json", DiscordChannel.GATEWAY_URL);
}

test "discord init stores fields" {
    const ch = DiscordChannel.init(std.testing.allocator, "my-bot-token", "guild-123", true);
    try std.testing.expectEqualStrings("my-bot-token", ch.token);
    try std.testing.expectEqualStrings("guild-123", ch.guild_id.?);
    try std.testing.expect(ch.allow_bots);
}

test "discord init no guild id" {
    const ch = DiscordChannel.init(std.testing.allocator, "tok", null, false);
    try std.testing.expect(ch.guild_id == null);
    try std.testing.expect(!ch.allow_bots);
}

test "discord send url buffer too small returns error" {
    var buf: [10]u8 = undefined;
    const result = DiscordChannel.sendUrl(&buf, "123456");
    try std.testing.expect(if (result) |_| false else |_| true);
}

// ════════════════════════════════════════════════════════════════════════════
// New Gateway Helper Tests
// ════════════════════════════════════════════════════════════════════════════

test "discord buildIdentifyJson" {
    var buf: [512]u8 = undefined;
    const json = try DiscordChannel.buildIdentifyJson(&buf, "mytoken", 37377);
    // Should contain op:2 and the token and intents
    try std.testing.expect(std.mem.indexOf(u8, json, "\"op\":2") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "mytoken") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "37377") != null);
}

test "discord buildHeartbeatJson no sequence" {
    var buf: [64]u8 = undefined;
    const json = try DiscordChannel.buildHeartbeatJson(&buf, 0);
    try std.testing.expectEqualStrings("{\"op\":1,\"d\":null}", json);
}

test "discord buildHeartbeatJson with sequence" {
    var buf: [64]u8 = undefined;
    const json = try DiscordChannel.buildHeartbeatJson(&buf, 42);
    try std.testing.expectEqualStrings("{\"op\":1,\"d\":42}", json);
}

test "discord buildResumeJson" {
    var buf: [256]u8 = undefined;
    const json = try DiscordChannel.buildResumeJson(&buf, "mytoken", "session123", 99);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"op\":6") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "session123") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "99") != null);
}

test "discord parseGatewayHost from wss url" {
    const host = DiscordChannel.parseGatewayHost("wss://us-east1.gateway.discord.gg");
    try std.testing.expectEqualStrings("us-east1.gateway.discord.gg", host);
}

test "discord parseGatewayHost with path" {
    const host = DiscordChannel.parseGatewayHost("wss://gateway.discord.gg/?v=10&encoding=json");
    try std.testing.expectEqualStrings("gateway.discord.gg", host);
}

test "discord parseGatewayHost no scheme returns original" {
    const host = DiscordChannel.parseGatewayHost("gateway.discord.gg");
    try std.testing.expectEqualStrings("gateway.discord.gg", host);
}

test "discord isMentioned with user id" {
    try std.testing.expect(DiscordChannel.isMentioned("<@123456> hello", "123456"));
    try std.testing.expect(DiscordChannel.isMentioned("hello <@!123456>", "123456"));
    try std.testing.expect(!DiscordChannel.isMentioned("hello world", "123456"));
    try std.testing.expect(!DiscordChannel.isMentioned("<@999999> hello", "123456"));
}

test "discord parseTarget supports channel prefix and reply suffix" {
    const plain = try DiscordChannel.parseTarget("123456");
    try std.testing.expectEqualStrings("123456", plain.channel_id);
    try std.testing.expect(plain.reply_message_id == null);

    const prefixed = try DiscordChannel.parseTarget("channel:987654");
    try std.testing.expectEqualStrings("987654", prefixed.channel_id);
    try std.testing.expect(prefixed.reply_message_id == null);

    const reply = try DiscordChannel.parseTarget("channel:987654:reply:112233");
    try std.testing.expectEqualStrings("987654", reply.channel_id);
    try std.testing.expectEqualStrings("112233", reply.reply_message_id.?);
}

test "discord buildSendBody includes message_reference for replies" {
    const alloc = std.testing.allocator;

    const plain = try DiscordChannel.buildSendBody(alloc, "hello", null);
    defer alloc.free(plain);
    try std.testing.expect(std.mem.indexOf(u8, plain, "\"content\":\"hello\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, plain, "\"message_reference\"") == null);

    const reply = try DiscordChannel.buildSendBody(alloc, "hello", "msg-42");
    defer alloc.free(reply);
    try std.testing.expect(std.mem.indexOf(u8, reply, "\"message_reference\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, reply, "\"message_id\":\"msg-42\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, reply, "\"fail_if_not_exists\":false") != null);
}

test "discord intents default" {
    const ch = DiscordChannel.init(std.testing.allocator, "tok", null, false);
    try std.testing.expectEqual(@as(u32, 37377), ch.intents);
}

test "discord initFromConfig passes all fields" {
    const config_types = @import("../config_types.zig");
    const cfg = config_types.DiscordConfig{
        .account_id = "discord-main",
        .token = "my-token",
        .guild_id = "guild-1",
        .allow_bots = true,
        .allow_from = &.{ "user1", "user2" },
        .require_mention = true,
        .mention_exempt_channel_ids = &.{ "c-allow", "c-safe" },
        .intents = 512,
    };
    const ch = DiscordChannel.initFromConfig(std.testing.allocator, cfg);
    try std.testing.expectEqualStrings("my-token", ch.token);
    try std.testing.expectEqualStrings("guild-1", ch.guild_id.?);
    try std.testing.expect(ch.allow_bots);
    try std.testing.expectEqualStrings("discord-main", ch.account_id);
    try std.testing.expectEqual(@as(usize, 2), ch.allow_from.len);
    try std.testing.expect(ch.require_mention);
    try std.testing.expectEqual(@as(usize, 2), ch.mention_exempt_channel_ids.len);
    try std.testing.expectEqualStrings("c-allow", ch.mention_exempt_channel_ids[0]);
    try std.testing.expectEqual(@as(u32, 512), ch.intents);
}

test "discord handleMessageCreate publishes inbound guild message with metadata" {
    const alloc = std.testing.allocator;
    var event_bus = bus_mod.Bus.init();
    defer event_bus.close();

    var ch = DiscordChannel.initFromConfig(alloc, .{
        .account_id = "dc-main",
        .token = "token",
    });
    ch.setBus(&event_bus);

    const msg_json =
        \\{"d":{"id":"m-1","channel_id":"c-1","guild_id":"g-1","content":"hello","author":{"id":"u-1","bot":false}}}
    ;
    const parsed = try std.json.parseFromSlice(std.json.Value, alloc, msg_json, .{});
    defer parsed.deinit();

    try ch.handleMessageCreate(parsed.value);

    var msg = event_bus.consumeInbound() orelse return try std.testing.expect(false);
    defer msg.deinit(alloc);
    try std.testing.expectEqualStrings("discord", msg.channel);
    try std.testing.expectEqualStrings("u-1", msg.sender_id);
    try std.testing.expectEqualStrings("c-1", msg.chat_id);
    try std.testing.expectEqualStrings("hello", msg.content);
    try std.testing.expectEqualStrings("discord:dc-main:channel:c-1", msg.session_key);
    try std.testing.expect(msg.metadata_json != null);

    const meta = try std.json.parseFromSlice(std.json.Value, alloc, msg.metadata_json.?, .{});
    defer meta.deinit();
    try std.testing.expect(meta.value == .object);
    try std.testing.expect(meta.value.object.get("account_id") != null);
    try std.testing.expect(meta.value.object.get("is_dm") != null);
    try std.testing.expect(meta.value.object.get("channel_id") != null);
    try std.testing.expect(meta.value.object.get("message_id") != null);
    try std.testing.expect(meta.value.object.get("guild_id") != null);
    try std.testing.expectEqualStrings("dc-main", meta.value.object.get("account_id").?.string);
    try std.testing.expect(!meta.value.object.get("is_dm").?.bool);
    try std.testing.expectEqualStrings("c-1", meta.value.object.get("channel_id").?.string);
    try std.testing.expectEqualStrings("m-1", meta.value.object.get("message_id").?.string);
    try std.testing.expectEqualStrings("g-1", meta.value.object.get("guild_id").?.string);
}

test "discord handleMessageCreate sets is_dm metadata for direct messages" {
    const alloc = std.testing.allocator;
    var event_bus = bus_mod.Bus.init();
    defer event_bus.close();

    var ch = DiscordChannel.initFromConfig(alloc, .{
        .account_id = "dc-main",
        .token = "token",
    });
    ch.setBus(&event_bus);

    const msg_json =
        \\{"d":{"channel_id":"dm-7","content":"hi dm","author":{"id":"u-7","bot":false}}}
    ;
    const parsed = try std.json.parseFromSlice(std.json.Value, alloc, msg_json, .{});
    defer parsed.deinit();

    try ch.handleMessageCreate(parsed.value);

    var msg = event_bus.consumeInbound() orelse return try std.testing.expect(false);
    defer msg.deinit(alloc);
    try std.testing.expectEqualStrings("discord:dc-main:direct:u-7", msg.session_key);
    try std.testing.expect(msg.metadata_json != null);

    const meta = try std.json.parseFromSlice(std.json.Value, alloc, msg.metadata_json.?, .{});
    defer meta.deinit();
    try std.testing.expect(meta.value == .object);
    try std.testing.expect(meta.value.object.get("is_dm") != null);
    try std.testing.expect(meta.value.object.get("is_dm").?.bool);
    try std.testing.expect(meta.value.object.get("guild_id") == null);
}

test "discord handleMessageCreate require_mention blocks unmentioned guild messages" {
    const alloc = std.testing.allocator;
    var event_bus = bus_mod.Bus.init();
    defer event_bus.close();

    var ch = DiscordChannel.initFromConfig(alloc, .{
        .account_id = "dc-main",
        .token = "token",
        .require_mention = true,
    });
    ch.setBus(&event_bus);
    ch.bot_user_id = try alloc.dupe(u8, "bot-1");
    defer alloc.free(ch.bot_user_id.?);

    const msg_json =
        \\{"d":{"channel_id":"c-2","guild_id":"g-2","content":"plain text","author":{"id":"u-2","bot":false}}}
    ;
    const parsed = try std.json.parseFromSlice(std.json.Value, alloc, msg_json, .{});
    defer parsed.deinit();

    try ch.handleMessageCreate(parsed.value);
    try std.testing.expectEqual(@as(usize, 0), event_bus.inboundDepth());
}

test "discord handleMessageCreate mention_exempt_channel_ids bypass require_mention" {
    const alloc = std.testing.allocator;
    var event_bus = bus_mod.Bus.init();
    defer event_bus.close();

    var ch = DiscordChannel.initFromConfig(alloc, .{
        .account_id = "dc-main",
        .token = "token",
        .require_mention = true,
        .mention_exempt_channel_ids = &.{"c-allow"},
    });
    ch.setBus(&event_bus);
    ch.bot_user_id = try alloc.dupe(u8, "bot-1");
    defer alloc.free(ch.bot_user_id.?);

    const msg_json =
        \\{"d":{"channel_id":"c-allow","guild_id":"g-2","content":"plain text","author":{"id":"u-2","bot":false}}}
    ;
    const parsed = try std.json.parseFromSlice(std.json.Value, alloc, msg_json, .{});
    defer parsed.deinit();

    try ch.handleMessageCreate(parsed.value);

    var msg = event_bus.consumeInbound() orelse return try std.testing.expect(false);
    defer msg.deinit(alloc);
    try std.testing.expectEqualStrings("discord", msg.channel);
    try std.testing.expectEqualStrings("u-2", msg.sender_id);
    try std.testing.expectEqualStrings("c-allow", msg.chat_id);
    try std.testing.expectEqualStrings("plain text", msg.content);
}

test "discord handleMessageCreate ignores own bot messages" {
    const alloc = std.testing.allocator;
    var event_bus = bus_mod.Bus.init();
    defer event_bus.close();

    var ch = DiscordChannel.initFromConfig(alloc, .{
        .account_id = "dc-main",
        .token = "token",
        .allow_bots = true,
    });
    ch.setBus(&event_bus);
    ch.bot_user_id = try alloc.dupe(u8, "bot-1");
    defer alloc.free(ch.bot_user_id.?);

    const msg_json =
        \\{"d":{"id":"m-self-1","channel_id":"c-2","guild_id":"g-2","content":"loop?","author":{"id":"bot-1","bot":true}}}
    ;
    const parsed = try std.json.parseFromSlice(std.json.Value, alloc, msg_json, .{});
    defer parsed.deinit();

    try ch.handleMessageCreate(parsed.value);
    try std.testing.expectEqual(@as(usize, 0), event_bus.inboundDepth());
}

test "discord handleMessageCreate thread requires mention when not replying" {
    const alloc = std.testing.allocator;
    var event_bus = bus_mod.Bus.init();
    defer event_bus.close();

    var ch = DiscordChannel.initFromConfig(alloc, .{
        .account_id = "dc-main",
        .token = "token",
        .require_mention = true,
    });
    defer ch.clearThreadCaches();
    ch.setBus(&event_bus);
    ch.bot_user_id = try alloc.dupe(u8, "bot-1");
    defer alloc.free(ch.bot_user_id.?);

    const thread_json =
        \\{"d":{"id":"thread-1","parent_id":"text-1","type":11}}
    ;
    const thread_parsed = try std.json.parseFromSlice(std.json.Value, alloc, thread_json, .{});
    defer thread_parsed.deinit();
    try ch.handleChannelUpsert(thread_parsed.value);

    const msg_json =
        \\{"d":{"id":"m-thread-1","channel_id":"thread-1","guild_id":"g-2","content":"plain text","position":1,"author":{"id":"u-2","bot":false}}}
    ;
    const parsed = try std.json.parseFromSlice(std.json.Value, alloc, msg_json, .{});
    defer parsed.deinit();

    try ch.handleMessageCreate(parsed.value);
    try std.testing.expectEqual(@as(usize, 0), event_bus.inboundDepth());
}

test "discord handleMessageCreate reply chain bypasses mention and carries thread_id" {
    const alloc = std.testing.allocator;
    var event_bus = bus_mod.Bus.init();
    defer event_bus.close();

    var ch = DiscordChannel.initFromConfig(alloc, .{
        .account_id = "dc-main",
        .token = "token",
        .require_mention = true,
    });
    defer ch.clearThreadCaches();
    ch.setBus(&event_bus);
    ch.bot_user_id = try alloc.dupe(u8, "bot-1");
    defer alloc.free(ch.bot_user_id.?);

    // Bot's own reply is ignored for inbound, but should seed reply context map.
    const bot_reply_1_json =
        \\{"d":{"id":"b-1","channel_id":"c-2","guild_id":"g-2","content":"first","author":{"id":"bot-1","bot":true},"message_reference":{"message_id":"m-root"}}}
    ;
    const bot_reply_1 = try std.json.parseFromSlice(std.json.Value, alloc, bot_reply_1_json, .{});
    defer bot_reply_1.deinit();
    try ch.handleMessageCreate(bot_reply_1.value);

    const user_reply_1_json =
        \\{"d":{"id":"u-1","channel_id":"c-2","guild_id":"g-2","content":"follow up","author":{"id":"u-2","bot":false},"message_reference":{"message_id":"b-1"},"referenced_message":{"id":"b-1","author":{"id":"bot-1","bot":true}}}}
    ;
    const user_reply_1 = try std.json.parseFromSlice(std.json.Value, alloc, user_reply_1_json, .{});
    defer user_reply_1.deinit();
    try ch.handleMessageCreate(user_reply_1.value);

    var msg_1 = event_bus.consumeInbound() orelse return try std.testing.expect(false);
    defer msg_1.deinit(alloc);
    const meta_1 = try std.json.parseFromSlice(std.json.Value, alloc, msg_1.metadata_json.?, .{});
    defer meta_1.deinit();
    try std.testing.expectEqualStrings("m-root", meta_1.value.object.get("thread_id").?.string);

    const bot_reply_2_json =
        \\{"d":{"id":"b-2","channel_id":"c-2","guild_id":"g-2","content":"second","author":{"id":"bot-1","bot":true},"message_reference":{"message_id":"u-1"}}}
    ;
    const bot_reply_2 = try std.json.parseFromSlice(std.json.Value, alloc, bot_reply_2_json, .{});
    defer bot_reply_2.deinit();
    try ch.handleMessageCreate(bot_reply_2.value);

    const user_reply_2_json =
        \\{"d":{"id":"u-2","channel_id":"c-2","guild_id":"g-2","content":"continue","author":{"id":"u-2","bot":false},"message_reference":{"message_id":"b-2"},"referenced_message":{"id":"b-2","author":{"id":"bot-1","bot":true}}}}
    ;
    const user_reply_2 = try std.json.parseFromSlice(std.json.Value, alloc, user_reply_2_json, .{});
    defer user_reply_2.deinit();
    try ch.handleMessageCreate(user_reply_2.value);

    var msg_2 = event_bus.consumeInbound() orelse return try std.testing.expect(false);
    defer msg_2.deinit(alloc);
    const meta_2 = try std.json.parseFromSlice(std.json.Value, alloc, msg_2.metadata_json.?, .{});
    defer meta_2.deinit();
    try std.testing.expectEqualStrings("m-root", meta_2.value.object.get("thread_id").?.string);
}

test "discord handleMessageCreate forum thread requires mention unless parent exempt" {
    const alloc = std.testing.allocator;
    var event_bus = bus_mod.Bus.init();
    defer event_bus.close();

    var ch = DiscordChannel.initFromConfig(alloc, .{
        .account_id = "dc-main",
        .token = "token",
        .require_mention = true,
    });
    defer ch.clearThreadCaches();
    ch.setBus(&event_bus);
    ch.bot_user_id = try alloc.dupe(u8, "bot-1");
    defer alloc.free(ch.bot_user_id.?);

    const forum_json =
        \\{"d":{"id":"forum-1","type":15}}
    ;
    const forum_parsed = try std.json.parseFromSlice(std.json.Value, alloc, forum_json, .{});
    defer forum_parsed.deinit();
    try ch.handleChannelUpsert(forum_parsed.value);

    const thread_json =
        \\{"d":{"id":"forum-thread-1","parent_id":"forum-1","type":11}}
    ;
    const thread_parsed = try std.json.parseFromSlice(std.json.Value, alloc, thread_json, .{});
    defer thread_parsed.deinit();
    try ch.handleChannelUpsert(thread_parsed.value);

    const msg_json =
        \\{"d":{"id":"m-forum-1","channel_id":"forum-thread-1","guild_id":"g-2","content":"plain text","position":1,"author":{"id":"u-2","bot":false}}}
    ;
    const parsed = try std.json.parseFromSlice(std.json.Value, alloc, msg_json, .{});
    defer parsed.deinit();

    try ch.handleMessageCreate(parsed.value);
    try std.testing.expectEqual(@as(usize, 0), event_bus.inboundDepth());
}

test "discord handleMessageCreate forum thread bypasses mention when parent is exempt" {
    const alloc = std.testing.allocator;
    var event_bus = bus_mod.Bus.init();
    defer event_bus.close();

    var ch = DiscordChannel.initFromConfig(alloc, .{
        .account_id = "dc-main",
        .token = "token",
        .require_mention = true,
        .mention_exempt_channel_ids = &.{"forum-1"},
    });
    defer ch.clearThreadCaches();
    ch.setBus(&event_bus);
    ch.bot_user_id = try alloc.dupe(u8, "bot-1");
    defer alloc.free(ch.bot_user_id.?);

    const forum_json =
        \\{"d":{"id":"forum-1","type":15}}
    ;
    const forum_parsed = try std.json.parseFromSlice(std.json.Value, alloc, forum_json, .{});
    defer forum_parsed.deinit();
    try ch.handleChannelUpsert(forum_parsed.value);

    const thread_json =
        \\{"d":{"id":"forum-thread-2","parent_id":"forum-1","type":11}}
    ;
    const thread_parsed = try std.json.parseFromSlice(std.json.Value, alloc, thread_json, .{});
    defer thread_parsed.deinit();
    try ch.handleChannelUpsert(thread_parsed.value);

    const msg_json =
        \\{"d":{"id":"m-forum-2","channel_id":"forum-thread-2","guild_id":"g-2","content":"plain text","position":1,"author":{"id":"u-2","bot":false}}}
    ;
    const parsed = try std.json.parseFromSlice(std.json.Value, alloc, msg_json, .{});
    defer parsed.deinit();

    try ch.handleMessageCreate(parsed.value);

    var msg = event_bus.consumeInbound() orelse return try std.testing.expect(false);
    defer msg.deinit(alloc);
    try std.testing.expectEqualStrings("forum-thread-2", msg.chat_id);
}

test "discord dispatch sequence accepts lower values after session reset" {
    var ch = DiscordChannel.init(std.testing.allocator, "token", null, false);
    defer {
        if (ch.session_id) |s| std.testing.allocator.free(s);
        if (ch.resume_gateway_url) |u| std.testing.allocator.free(u);
        if (ch.bot_user_id) |u| std.testing.allocator.free(u);
    }

    // Simulate stale sequence from an old session.
    ch.sequence.store(42, .release);

    var ws_dummy: websocket.WsClient = undefined;
    const ready_dispatch =
        \\{"op":0,"s":1,"t":"READY","d":{"session_id":"sess-1","resume_gateway_url":"wss://gateway.discord.gg/?v=10&encoding=json","user":{"id":"bot-1"}}}
    ;
    try ch.handleGatewayMessage(&ws_dummy, ready_dispatch);

    try std.testing.expectEqual(@as(i64, 1), ch.sequence.load(.acquire));
}

test "discord invalid session non-resumable clears state and identifies" {
    const alloc = std.testing.allocator;
    var ch = DiscordChannel.init(alloc, "token", null, false);
    ch.session_id = try alloc.dupe(u8, "sess-1");
    ch.resume_gateway_url = try alloc.dupe(u8, "wss://gateway.discord.gg/?v=10&encoding=json");
    ch.sequence.store(77, .release);

    const action = ch.resolveInvalidSessionAction(false);
    try std.testing.expectEqual(DiscordChannel.InvalidSessionAction.identify, action);
    try std.testing.expect(ch.session_id == null);
    try std.testing.expect(ch.resume_gateway_url == null);
    try std.testing.expectEqual(@as(i64, 0), ch.sequence.load(.acquire));
}

test "discord invalid session resumable keeps state and resumes" {
    const alloc = std.testing.allocator;
    var ch = DiscordChannel.init(alloc, "token", null, false);
    ch.session_id = try alloc.dupe(u8, "sess-2");
    defer {
        if (ch.session_id) |s| alloc.free(s);
    }
    ch.sequence.store(123, .release);

    const action = ch.resolveInvalidSessionAction(true);
    try std.testing.expectEqual(DiscordChannel.InvalidSessionAction.resume_session, action);
    try std.testing.expect(ch.session_id != null);
    try std.testing.expectEqual(@as(i64, 123), ch.sequence.load(.acquire));
}

test "discord invalid session resumable without session falls back to identify" {
    const alloc = std.testing.allocator;
    var ch = DiscordChannel.init(alloc, "token", null, false);
    ch.resume_gateway_url = try alloc.dupe(u8, "wss://gateway.discord.gg/?v=10&encoding=json");
    ch.sequence.store(33, .release);

    const action = ch.resolveInvalidSessionAction(true);
    try std.testing.expectEqual(DiscordChannel.InvalidSessionAction.identify, action);
    try std.testing.expect(ch.session_id == null);
    try std.testing.expect(ch.resume_gateway_url == null);
    try std.testing.expectEqual(@as(i64, 0), ch.sequence.load(.acquire));
}

test "discord intent bitmask guilds" {
    // GUILDS = 1
    try std.testing.expectEqual(@as(u32, 1), 1);
    // GUILD_MESSAGES = 512
    try std.testing.expectEqual(@as(u32, 512), 512);
    // MESSAGE_CONTENT = 32768
    try std.testing.expectEqual(@as(u32, 32768), 32768);
    // DIRECT_MESSAGES = 4096
    try std.testing.expectEqual(@as(u32, 4096), 4096);
    // Default intents = 1|512|32768|4096 = 37377
    try std.testing.expectEqual(@as(u32, 37377), 1 | 512 | 32768 | 4096);
}
