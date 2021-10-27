//! Wire Serialisation Object Notation
//! Example:
//! name: string, age: int, sex: int, habit: ?[]string, using: bool, access: {bedroom: bool, bathroom: bool};
//! Person,64,0,[Singing, Painting],T,{T, F};
//! Pokson,89,1,null,F,{F, F};

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
};

const OptionalType = struct {
    child: Type,
};

const ArrayType = struct {
    child: Type,
};

const MapType = struct {
    children: []const Field,
};

const Field = struct {
    name: []const u8,
    type_: Type,
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

    pub fn free(self: *Self, fields: []const Field) void {
        for (fields) |f| {
            switch (f.type_) {
                .Optional => |v| self.alloc.destroy(v),
                .Array => |v| self.alloc.destroy(v),
                .Map => |v| {
                    self.free(v.children);
                    self.alloc.destroy(v);
                },
                else => {},
            }
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
}
