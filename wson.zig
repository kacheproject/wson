//! Wire Serialisation Object Notation
//! Example:
//! name: string, age: int, sex: int, habit: ?[]string, using: bool, access: {bedroom: bool, bathroom: bool};
//! Person,64,0,[Singing, Painting],T,{T, F};
//! Pokson,89,1,,F,{F, F};

const std = @import("std");
const Allocator = std.mem.Allocator;

const Type = union(enum) {
    String: void,
    Int: void,
    Float: void,
    Bool: void,
    Optional: *OptionalType,
    Array: *ArrayType,
    Map: *MapType,

    const Self = @This();

    fn eql(self: *const Self, other: *const Self) bool {
        if (std.meta.activeTag(self.*) == std.meta.activeTag(other.*)) {
            return switch(self.*) {
                .Optional => OptionalType.eql(self.Optional, other.Optional),
                .Array => ArrayType.eql(self.Array, other.Array),
                .Map => MapType.eql(self.Map, other.Map),
                else => true,
            };
        } else return false;
    }
};

const OptionalType = struct {
    child: Type,

    const Self = @This();

    fn eql(self: *const Self, other: *const Self) bool {
        return Type.eql(&self.child, &other.child);
    }
};

const ArrayType = struct {
    child: Type,

    const Self = @This();

    fn eql(self: *const Self, other: *const Self) bool {
        return Type.eql(&self.child, &other.child);
    }
};

const MapType = struct {
    children: []const Field,

    const Self = @This();

    fn eql(self: *const Self, other: *const Self) bool {
        if (self.children.len == other.children.len) {
            for (self.children) |*child, i| {
                if (!child.eql(&other.children[i])) {
                    return false;
                }
            }
        } else return false;
        return true;
    }
};

const Field = struct {
    name: []const u8,
    type_: Type,

    const Self = @This();

    fn eql(self: *const Self, other: *const Field) bool {
        return Type.eql(&self.type_, &other.type_) and std.mem.eql(u8, self.name, other.name);
    }
};

const SchemeParser = struct {
    alloc: *Allocator,

    pub fn init(alloc: *Allocator) Self {
        return Self {
            .alloc = alloc,
        };
    }

    const Self = @This();

    const Error = error {
        BadSyntax,
    } || Allocator.Error;

    fn parseType(self: *Self, s: []const u8) Error!Type {
        if (s[0] == ' ' or s[s.len-1] == ' ') return try self.parseType(std.mem.trim(u8, s, " "));
        if (s[0] == '?') {
            var val = try self.alloc.create(OptionalType);
            errdefer self.alloc.destroy(val);
            val.child = try self.parseType(s[1..s.len]);
            return Type {.Optional = val};
        } else if (s[0] == '[' and s[1] == ']') {
            var val = try self.alloc.create(ArrayType);
            errdefer self.alloc.destroy(val);
            val.child = try self.parseType(s[2..s.len]);
            return Type {.Array = val};
        } else if (std.mem.eql(u8, s, "int")){
            return .Int;
        } else if (std.mem.eql(u8, s, "bool")){
            return .Bool;
        } else if (std.mem.eql(u8, s, "float")){
            return .Float;
        } else if (s[0] == '{' and s[s.len-1] == '}') {
            var val = try self.alloc.create(MapType);
            errdefer self.alloc.destroy(val);
            val.children = try self.parseFields(s[1..s.len-1]);
            return Type {.Map = val};
        } else if (std.mem.eql(u8, s, "string")) {
            return .String;
        } else {
            return Error.BadSyntax;
        }
    }

    fn parseField(self: *Self, s: []const u8) Error!Field {
        if (s[0] == ' ' or s[s.len-1] == ' ') return try self.parseField(std.mem.trim(u8, s, " "));
        var fieldNameEnd = s.len;
        var type_: ?Type = null;
        for (s) |c, i| {
            switch (c) {
                ':' => {
                    // With type
                    fieldNameEnd = i;
                    type_ = try self.parseType(s[i+1..s.len]);
                    break;
                },
                else => {},
            }
        }
        return Field {
            .name = s[0..fieldNameEnd],
            .type_ = type_ orelse Type.String,
        };
    }

    const FieldsParsingState = enum {
        Field,
        MapEscape,
    };

    fn parseFields(self: *Self, s: []const u8) Error![]Field {
        var list = std.ArrayList(Field).init(self.alloc);
        errdefer list.deinit();
        var lastStart = @as(usize, 0);
        var state = FieldsParsingState.Field;
        for (s) |c, i| {
            switch (state) {
                .Field => switch (c) {
                    ',' => {
                        if (i - lastStart <= 1) return Error.BadSyntax;
                        var fieldStr = s[lastStart..i];
                        try list.append(try self.parseField(fieldStr));
                        lastStart = i+1;
                    },
                    ';' => {
                        break;
                    },
                    '{' => {
                        state = .MapEscape;
                    }, 
                    else => {},
                },
                .MapEscape => {
                    if (c=='}') state = .Field;
                }
            }
        }
        if (s.len - lastStart > 1) {
            var fieldStr = s[lastStart..s.len];
            try list.append(try self.parseField(fieldStr));
            lastStart = s.len+1;
        } else if (s[s.len-1] != ',') {
            return Error.BadSyntax;
        }
        return list.toOwnedSlice();
    }

    pub fn parse(self: *Self, s: []const u8) Error![]Field {
        if (s[s.len-1] == ';') {
            return try self.parseFields(s[0..s.len-1]);
        } else {
            return Error.BadSyntax;
        }
    }

    fn freeType(self: *Self, t: Type) void {
        switch (t) {
            .Optional => |v| {
                self.freeType(v.child);
                self.alloc.destroy(v);
            },
            .Array => |v| self.alloc.destroy(v),
            .Map => |v| {
                self.free(v.children);
                self.alloc.destroy(v);
            },
            else => {},
        }
    }

    pub fn free(self: *Self, fields: []const Field) void {
        for (fields) |f| {
            self.freeType(f.type_);
        }
        self.alloc.free(fields);
    }
};

test "SchemeParser can parse name" {
    const _t = std.testing;
    {
        const SCHEME = "name: string;";
        var parser = SchemeParser.init(_t.allocator);
        var fields = try parser.parse(SCHEME);
        defer parser.free(fields);
        try _t.expectEqualStrings("name", fields[0].name);
    }
}

test "SchemeParser can parse basic types" {
    const _t = std.testing; // basic types: int, float, string, bool
    {
        const SCHEME = "age: int;";
        var parser = SchemeParser.init(_t.allocator);
        var fields = try parser.parse(SCHEME);
        defer parser.free(fields);
        try _t.expectEqual(Type.Int, fields[0].type_);
    }
    {
        const SCHEME = "volume: float;";
        var parser = SchemeParser.init(_t.allocator);
        var fields = try parser.parse(SCHEME);
        defer parser.free(fields);
        try _t.expectEqual(Type.Float, fields[0].type_);
    }
    {
        const SCHEME = "name: string;";
        var parser = SchemeParser.init(_t.allocator);
        var fields = try parser.parse(SCHEME);
        defer parser.free(fields);
        try _t.expectEqual(Type.String, fields[0].type_);
    }
    {
        const SCHEME = "name;";
        var parser = SchemeParser.init(_t.allocator);
        var fields = try parser.parse(SCHEME);
        defer parser.free(fields);
        try _t.expectEqual(Type.String, fields[0].type_);
    }
    {
        const SCHEME = "yes: bool;";
        var parser = SchemeParser.init(_t.allocator);
        var fields = try parser.parse(SCHEME);
        defer parser.free(fields);
        try _t.expectEqual(Type.Bool, fields[0].type_);
    }
}

test "SchemeParser report bad syntax with no name field" {
    const _t = std.testing;
    { // Won't report error if ; is next to ,
        const SCHEME = "name,;";
        var parser = SchemeParser.init(_t.allocator);
        var fields = try parser.parse(SCHEME);
        defer parser.free(fields);
    }
    { // BadSyntax if there is a field without name or type
        const SCHEME = "name,,age;";
        var parser = SchemeParser.init(_t.allocator);
        try _t.expectError(SchemeParser.Error.BadSyntax, parser.parse(SCHEME));
    }
}

test "SchemeParser can parse optional type" {
    const _t = std.testing;
    {
        const SCHEME = "age: ?int;";
        var parser = SchemeParser.init(_t.allocator);
        var fields = try parser.parse(SCHEME);
        defer parser.free(fields);
        try _t.expectEqual(Type.Optional, fields[0].type_);
        try _t.expectEqual(Type.Int, fields[0].type_.Optional.child);
    }
}

test "SchemeParser can parse array type" {
    const _t = std.testing;
    {
        const SCHEME = "prices: []int;";
        var parser = SchemeParser.init(_t.allocator);
        var fields = try parser.parse(SCHEME);
        defer parser.free(fields);
        try _t.expectEqual(Type.Array, fields[0].type_);
        try _t.expectEqual(Type.Int, fields[0].type_.Array.child);
    }
}

test "SchemeParser can parse map type" {
    const _t = std.testing;
    {
        const SCHEME = "access: {bedroom: bool, bathroom: bool};";
        var parser = SchemeParser.init(_t.allocator);
        var fields = try parser.parse(SCHEME);
        defer parser.free(fields);
        try _t.expectEqual(@as(usize, 2), fields[0].type_.Map.children.len);
        try _t.expectEqual(Type.Map, fields[0].type_);
        try _t.expectEqual(Type.Bool, fields[0].type_.Map.children[0].type_);
    }
    {
        const SCHEME = "access: {bedroom: bool, bathroom: {water_switch: bool, light_switch: bool}};";
        var parser = SchemeParser.init(_t.allocator);
        var fields = try parser.parse(SCHEME);
        defer parser.free(fields);
        try _t.expectEqual(@as(usize, 2), fields[0].type_.Map.children.len);
        try _t.expectEqual(Type.Map, fields[0].type_);
        try _t.expectEqual(Type.Map, fields[0].type_.Map.children[1].type_);
    }
}

const FixedBufferStream = std.io.FixedBufferStream;

const ESCAPE_CHARS = [_]u8{'\\', ','};

const DataBuilder = struct {
    scheme: []const Field,
    values: []FieldValue,
    nextFieldPos: usize = 0,
    alloc: std.heap.ArenaAllocator,
    backendAlloc: *Allocator,

    const Self = @This();

    const FieldValue = union(enum) {
        String: []const u8,
        Int: i64,
        Float: f64,
        Bool: bool,
        Null: void,
        Array: []const FieldValue,
        Map: *DataBuilder,
    };

    const BuildConfig = struct {
        noEndMark: bool = false, // if it's true, the builder will no write the ';' at the end of the values

        fn merge(self: BuildConfig, other: BuildConfig, comptime fieldNames: anytype) BuildConfig {
            var copy = self;
            inline for (std.meta.fields(@TypeOf(fieldNames))) |field| {
                const name = @field(fieldNames, field.name);
                @field(copy, name) = @field(other, name);
            }
            return copy;
        }
    };

    const Error = error {
        BadValue,
    };

    fn ensureArrayType(expectedElType: Type, arr: []const FieldValue) bool {
        for (arr) |arrEl| {
            if (!ensureType(expectedElType, arrEl)) {
                return false;
            }
        }
        return true;
    }

    fn ensureType(expected: Type, actual: FieldValue) bool {
        return switch (expected) {
            .String => actual == .String,
            .Int => actual == .Int,
            .Float => actual == .Float,
            .Bool => actual == .Bool,
            .Optional => |optionalInfo| actual == .Null or ensureType(optionalInfo.child, actual),
            .Array => |arrInfo| actual == .Array and ensureArrayType(arrInfo.child, actual.Array),
            .Map => |mapInfo| actual == .Map and MapType.eql(&.{.children = actual.Map.scheme}, mapInfo),
        };
    }

    fn isEscapeChar(ch: u8) bool {
        inline for (ESCAPE_CHARS) |esch| {
            if (esch == ch) return true;
        }
        return false;
    }

    fn isEndOfFields(self: *Self) bool {
        return self.nextFieldPos >= self.scheme.len;
    }

    fn writeValue(fieldType: Type, fieldVal: FieldValue, bufStream: *FixedBufferStream([]u8), config: BuildConfig) (FixedBufferStream([]u8).WriteError||Error||Allocator.Error||error{EOF})!void {
        var writer = bufStream.writer();
        const nestedConfig = config.merge(BuildConfig {.noEndMark=true}, .{"noEndMark"});
        switch (fieldType) {
            .String => {
                for (fieldVal.String) |ch| {
                    if (isEscapeChar(ch)) {
                        try writer.writeIntNative(u8, '\\');
                    }
                    try writer.writeIntNative(u8, ch);
                }
            },
            .Bool => {
                try writer.writeIntNative(u8, @as(u8, if (fieldVal.Bool) 'T' else 'F'));
            },
            .Int => {
                try std.fmt.format(writer, "{}", .{fieldVal.Int});
            },
            .Float => {
                try std.fmt.format(writer, "{}", .{fieldVal.Float});
            },
            .Optional => {
                if (fieldVal == .Null) {
                    // Do nothing
                } else {
                    try writeValue(fieldType.Optional.child, fieldVal, bufStream, nestedConfig);
                }
            },
            .Array => {
                try writer.writeIntNative(u8, '[');
                var arr = fieldVal.Array;
                for (arr) |item, i| {
                    try writeValue(fieldType.Array.child, item, bufStream, nestedConfig);
                    if (i != (arr.len-1)) {
                        try writer.writeIntNative(u8, ',');
                    }
                }
                try writer.writeIntNative(u8, ']');
            },
            .Map => {
                var builder = fieldVal.Map;
                try writer.writeIntNative(u8, '{');
                try builder.build(bufStream, nestedConfig);
                try writer.writeIntNative(u8, '}');
            }
        }
    }

    pub fn buildNext(self: *Self, bufStream: *FixedBufferStream([]u8), config: BuildConfig) !void {
        _ = config;
        if (self.isEndOfFields())
            return error.EOF;
        const pos = self.nextFieldPos;

        var fieldVal = self.values[pos];
        var field = self.scheme[pos];
        if (!ensureType(field.type_, fieldVal)) {
            return Error.BadValue;
        }
        const oldPos = bufStream.pos;
        errdefer bufStream.seekTo(oldPos) catch unreachable;
        try writeValue(field.type_, fieldVal, bufStream, .{});
        self.nextFieldPos += 1;
        errdefer self.nextFieldPos -= 1;
        if (self.isEndOfFields()) {
            if (!config.noEndMark) _ = try bufStream.write(";");
        } else {
            _ = try bufStream.write(",");
        }
    }

    fn calcRequiredSizeOf(typ: Type, fieldVal: FieldValue, config: BuildConfig) Error!usize {
        var size = @as(usize, 0);
        const nestedConfig = config.merge(BuildConfig {.noEndMark=true}, .{"noEndMark"});
        switch(typ) {
            .String => {
                for (fieldVal.String) |ch| {
                    if (isEscapeChar(ch)) {
                        size += 1;
                    }
                    size += 1;
                }
            },
            .Bool => {
                size += 1;
            },
            .Int => {
                size += std.fmt.count("{}", .{fieldVal.Int});
            },
            .Float => {
                size += std.fmt.count("{}", .{fieldVal.Float});
            },
            .Optional => {
                if (fieldVal != .Null) {
                    size += try calcRequiredSizeOf(typ.Optional.child, fieldVal, nestedConfig);
                }
            },
            .Array => {
                size += 1;
                var arr = fieldVal.Array;
                for (arr) |item, i| {
                    size += try calcRequiredSizeOf(typ.Array.child, item, nestedConfig);
                    if (i != (arr.len-1)) {
                        size += 1;
                    }
                }
                size += 1;
            },
            .Map => {
                var builder = fieldVal.Map;
                size += 1;
                size += try builder.calcRequiredSize(nestedConfig);
                size += 1;
            }
        }
        return size;
    }

    pub fn calcRequiredSizeFor(self: *Self, pos: usize, config: BuildConfig) !usize {
        var size = @as(usize, 0);
        var field = self.scheme[pos];
        var fieldVal = self.values[pos];
        if (ensureType(field.type_, fieldVal)) {
            size += try calcRequiredSizeOf(field.type_, fieldVal, config);
        } else return Error.BadValue;
        if (pos == self.scheme.len-1) {
            if (!config.noEndMark) size += 1;
        } else {
            size += 1;
        }
        return size;
    }

    pub fn calcRequiredSize(self: *Self, config: BuildConfig) !usize {
        var size = @as(usize, 0);
        for (self.scheme) |_, i| {
            size += try self.calcRequiredSizeFor(i, config);
        }
        return size;
    }

    pub fn build(self: *Self, writer: *FixedBufferStream([]u8), config: BuildConfig) !void {
        while (true) {
            self.buildNext(writer, config) catch |e| switch (e) {
                error.EOF => return,
                else => return e,
            };
        }
    }

    /// Caller owns `scheme` and `buf`. They should not be free'd for DataBuilder working.
    pub fn init(scheme: []const Field, alloc: *Allocator) Self {
        return Self {
            .scheme = scheme,
            .values = &.{},
            .alloc = std.heap.ArenaAllocator.init(alloc),
            .backendAlloc = alloc,
        };
    }

    pub fn reset(self: *Self) void {
        self.alloc.deinit();
        self.alloc = std.heap.ArenaAllocator.init(self.backendAlloc);
        self.nextFieldPos = 0;
        self.values = &.{};
    }

    pub fn deinit(self: *Self) void {
        self.alloc.deinit();
    }

    pub fn getFieldValue(self: *Self, comptime T: type, val: T) Allocator.Error!FieldValue {
        const info = @typeInfo(T);
        if (comptime std.meta.trait.isZigString(T)) {
            return FieldValue {.String = val};
        } else if (T == bool) {
            return FieldValue {.Bool = val};
        } else if (info == .Int) {
            return FieldValue {.Int = @intCast(i64, val)};
        } else if (info == .Optional) {
            if (val != null) {
                return self.getFieldValue(info.Optional.child, val.?);
            } else {
                return FieldValue.Null;
            }
        } else if (info == .Float) {
            return FieldValue {.Float = val};
        } else if (comptime std.meta.trait.isSlice(T)) {
            if (info.Pointer.child == FieldValue) {
                return FieldValue {.Array = val};
            } else {
                var values = try self.alloc.allocator.alloc(FieldValue, val.len);
                for (val) |item, i| {
                    values[i] = try self.getFieldValue(@TypeOf(item), item);
                }
                return FieldValue {.Array = values};
            }
        } else if (info == .Pointer and info.Pointer.child == DataBuilder) {
            return FieldValue {.Map = val};
        } else if (T == FieldValue) {
            return val;
        } else @compileError(comptime std.fmt.comptimePrint("unexpected type \'{}\'", .{T}));
    }

    pub fn getValues(self: *Self) Allocator.Error![]FieldValue {
        if (self.values.len == 0) {
            self.values = try self.alloc.allocator.alloc(FieldValue, self.scheme.len);
        }
        return self.values;
    }

    fn isAnyStringHashMap(comptime T: type) bool {
        const info = @typeInfo(T);
        return T == std.StringHashMap(FieldValue) or
            T == std.StringArrayHashMap(FieldValue) or
            (info == .Optional and isAnyStringHashMap(info.Optional.child));
    }

    pub fn put(self: *Self, comptime T: type, key: []const u8, val: T) !void {
        _ = try self.getValues();
        for (self.scheme) |field, i| {
            if (std.mem.eql(u8, field.name, key)) {
                if (comptime isAnyStringHashMap(T)) {
                    if (field.type_ != .Map and (field.type_ == .Optional and field.type_.Optional.child != .Map)) return Error.BadValue;
                    const typ = if (field.type_ == .Optional) field.type_.Optional.child else field.type_;
                    if (@typeInfo(T) != .Optional) {
                        var fieldVals = try self.alloc.allocator.alloc(FieldValue, val.count());
                        errdefer self.alloc.allocator.free(fieldVals);
                        var builder = try self.alloc.allocator.create(DataBuilder);
                        builder.* = DataBuilder.init(typ.Map.children, &self.alloc.allocator);
                        errdefer self.alloc.allocator.destroy(builder);
                        var iter = val.keyIterator();
                        while (iter.next()) |k| {
                            try builder.put(FieldValue, k.*, val.get(k.*).?);
                        }
                        self.values[i] = try self.getFieldValue(*DataBuilder, builder);
                    } else {
                        if (val) |nval| {
                            try self.put(@typeInfo(T).Optional.child, key, nval);
                        } else {
                            self.values[i] = try self.getFieldValue(FieldValue, FieldValue.Null);
                        }
                    }
                } else {
                    self.values[i] = try self.getFieldValue(T, val);
                }
                break;
            }
        }
    }
};

test "DataBuilder" {
    const t = std.testing;
    {
        const SCHEME_TEXT = "name: string, year: int, enabled: bool, main_characters: []string, buy_on: ?{steam: bool, epic: bool};";
        var schemeParser = SchemeParser.init(t.allocator);
        var scheme = try schemeParser.parse(SCHEME_TEXT);
        defer schemeParser.free(scheme);
        var dataBuilder = DataBuilder.init(scheme, t.allocator);
        defer dataBuilder.deinit();
        try dataBuilder.put([]const u8, "name", "Titanfall 2");
        try dataBuilder.put(i16, "year", 2016);
        try dataBuilder.put(bool, "enabled", true);
        var characters = [_][]const u8{"Jack Cooper", "BT-7274"};
        try dataBuilder.put([][]const u8, "main_characters", &characters);
        var buyOnTable = std.StringHashMap(DataBuilder.FieldValue).init(t.allocator);
        defer buyOnTable.deinit();
        try buyOnTable.put("steam", try dataBuilder.getFieldValue(bool, true));
        try buyOnTable.put("epic", try dataBuilder.getFieldValue(bool, false));
        try dataBuilder.put(std.StringHashMap(DataBuilder.FieldValue), "buy_on", buyOnTable);
        var resultBuf = std.mem.zeroes([2048:0]u8);
        var resultStream = std.io.fixedBufferStream(&resultBuf);
        try dataBuilder.build(&resultStream, .{});
        // std.debug.print("result=\"{s}\"\n", .{resultBuf});
        try t.expectEqualStrings("Titanfall 2,2016,T,[Jack Cooper,BT-7274],{T,F};", resultStream.getWritten());
    }
}

test "DataBuilder.calcRequiredSize can return exact size for result string" {
    const t = std.testing;
    {
        const SCHEME_TEXT = "name: string, year: int, enabled: bool, main_characters: []string, buy_on: ?{steam: bool, epic: bool};";
        var schemeParser = SchemeParser.init(t.allocator);
        var scheme = try schemeParser.parse(SCHEME_TEXT);
        defer schemeParser.free(scheme);
        var dataBuilder = DataBuilder.init(scheme, t.allocator);
        defer dataBuilder.deinit();
        try dataBuilder.put([]const u8, "name", "Titanfall 2");
        try dataBuilder.put(i16, "year", 2016);
        try dataBuilder.put(bool, "enabled", true);
        var characters = [_][]const u8{"Jack Cooper", "BT-7274"};
        try dataBuilder.put([][]const u8, "main_characters", &characters);
        var buyOnTable = std.StringHashMap(DataBuilder.FieldValue).init(t.allocator);
        defer buyOnTable.deinit();
        try buyOnTable.put("steam", try dataBuilder.getFieldValue(bool, true));
        try buyOnTable.put("epic", try dataBuilder.getFieldValue(bool, false));
        try dataBuilder.put(std.StringHashMap(DataBuilder.FieldValue), "buy_on", buyOnTable);
        try t.expectEqual(@as(usize, 47), try dataBuilder.calcRequiredSize(.{}));
    }
    {
        const SCHEME_TEXT = "name: string, year: int, enabled: bool, main_characters: []string, buy_on: ?{steam: bool, epic: bool};";
        var schemeParser = SchemeParser.init(t.allocator);
        var scheme = try schemeParser.parse(SCHEME_TEXT);
        defer schemeParser.free(scheme);
        var dataBuilder = DataBuilder.init(scheme, t.allocator);
        defer dataBuilder.deinit();
        try dataBuilder.put([]const u8, "name", "Titanfall 2");
        try dataBuilder.put(i16, "year", 2016);
        try dataBuilder.put(bool, "enabled", true);
        var characters = [_][]const u8{"Jack Cooper", "BT-7274"};
        try dataBuilder.put([][]const u8, "main_characters", &characters);
        try dataBuilder.put(?std.StringHashMap(DataBuilder.FieldValue), "buy_on", null);
        try t.expectEqual(@as(usize, 42), try dataBuilder.calcRequiredSize(.{}));
    }
}

const DataParser = struct {};
