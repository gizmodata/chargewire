// Converts spec/openapi.yaml to JSON at build time, so the SDK serves the OpenAPI document
// (spec §3.3) without a YAML library at run time or a scripting language at build time.
//
//   openapi_json <openapi.yaml> <out.cc> [<out.json>]
//
// It reads a strict subset of YAML, the one spec/openapi.yaml is written in, and refuses
// anything else with the line number, rather than guess:
//
//   - block mappings and block sequences (a sequence may sit at its key's indentation);
//   - flow sequences and mappings on one line ([a, "b"], {}, [x, null]);
//   - `|` and `|-` block scalars;
//   - double- and single-quoted scalars;
//   - plain scalars, resolved as PyYAML (YAML 1.1) resolves them: null, true/false and
//     decimal integers become JSON; every other plain scalar that YAML 1.1 would read as
//     something other than a string (yes/no/on/off, floats, octal, hex, sexagesimal numbers,
//     timestamps, `=`, `<<`) is refused, to be quoted;
//   - comments.
//
// No anchors, aliases, tags, directives, `>` scalars, multi-line plain or flow scalars,
// tabs, duplicate keys or keys that are not strings. The conformance suite compares the
// served document with PyYAML's reading of the same file (conformance/tests/test_openapi.py),
// so a difference between this reader and YAML fails there.
//
// <out.cc> defines warpsql::OpenApiJson() and warpsql::OpenApiEtag() (openapi_document.hpp);
// <out.json> is the JSON itself, for inspection.
#include <cstdint>
#include <cstdio>
#include <fstream>
#include <iostream>
#include <regex>
#include <sstream>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace {

struct Node {
  enum class Kind { kNull, kBool, kInt, kString, kSeq, kMap };
  Kind kind = Kind::kNull;
  bool boolean = false;
  std::string text;  // kInt (its digits) and kString
  std::vector<Node> items;
  std::vector<std::pair<std::string, Node>> entries;
};

struct Line {
  int number = 0;  // 1-based, for messages
  int indent = 0;
  std::string content;  // after the indentation
  bool blank = false;   // empty or a comment only
};

class YamlError : public std::runtime_error {
 public:
  YamlError(int line, const std::string& what)
      : std::runtime_error("line " + std::to_string(line) + ": " + what) {}
};

// ---- Plain scalars, resolved as PyYAML's implicit resolvers do ----

const std::regex kBool("^(?:true|True|TRUE|false|False|FALSE)$");
const std::regex kYaml11Bool("^(?:yes|Yes|YES|no|No|NO|on|On|ON|off|Off|OFF|y|Y|n|N)$");
const std::regex kNull("^(?:~|null|Null|NULL)$");
const std::regex kDecimal("^-?(?:0|[1-9][0-9]*)$");
const std::regex kYaml11Int(
    "^(?:[-+]?0b[0-1_]+|[-+]?0[0-7_]+|[-+]?(?:0|[1-9][0-9_]*)|[-+]?0x[0-9a-fA-F_]+|"
    "[-+]?[1-9][0-9_]*(?::[0-5]?[0-9])+)$");
const std::regex kYaml11Float(
    "^(?:[-+]?(?:[0-9][0-9_]*)\\.[0-9_]*(?:[eE][-+][0-9]+)?|\\.[0-9_]+(?:[eE][-+][0-9]+)?|"
    "[-+]?[0-9][0-9_]*(?::[0-5]?[0-9])+\\.[0-9_]*|[-+]?\\.(?:inf|Inf|INF)|\\.(?:nan|NaN|NAN))$");
const std::regex kYaml11Timestamp(
    "^(?:[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]|[0-9][0-9][0-9][0-9]-[0-9][0-9]?-[0-9][0-9]?"
    "(?:[Tt]|[ \\t]+)[0-9][0-9]?:[0-9][0-9]:[0-9][0-9](?:\\.[0-9]*)?"
    "(?:[ \\t]*(?:Z|[-+][0-9][0-9]?(?::[0-9][0-9])?))?)$");

Node ResolvePlain(const std::string& text, int line) {
  Node node;
  if (text.empty() || std::regex_match(text, kNull)) return node;
  if (std::regex_match(text, kBool)) {
    node.kind = Node::Kind::kBool;
    node.boolean = text[0] == 't' || text[0] == 'T';
    return node;
  }
  if (std::regex_match(text, kDecimal)) {
    node.kind = Node::Kind::kInt;
    node.text = text;
    return node;
  }
  if (std::regex_match(text, kYaml11Bool) || std::regex_match(text, kYaml11Int) ||
      std::regex_match(text, kYaml11Float) || std::regex_match(text, kYaml11Timestamp) ||
      text == "=" || text == "<<") {
    throw YamlError(line, "the plain scalar '" + text + "' is not a plain string in YAML 1.1: quote it");
  }
  static const std::string kIndicators = "&*!|>%@`?#,[]{}'\"";
  if (kIndicators.find(text[0]) != std::string::npos || (text[0] == '-' && (text.size() == 1 || text[1] == ' ')) ||
      (text[0] == ':' && (text.size() == 1 || text[1] == ' '))) {
    throw YamlError(line, "the plain scalar '" + text + "' starts with an indicator: quote it");
  }
  if (text.find(": ") != std::string::npos || text.find(" #") != std::string::npos ||
      text.back() == ':') {
    throw YamlError(line, "the plain scalar '" + text + "' is ambiguous: quote it");
  }
  node.kind = Node::Kind::kString;
  node.text = text;
  return node;
}

void AppendUtf8(std::string& out, uint32_t code) {
  if (code < 0x80) {
    out += static_cast<char>(code);
  } else if (code < 0x800) {
    out += static_cast<char>(0xC0 | (code >> 6));
    out += static_cast<char>(0x80 | (code & 0x3F));
  } else {
    out += static_cast<char>(0xE0 | (code >> 12));
    out += static_cast<char>(0x80 | ((code >> 6) & 0x3F));
    out += static_cast<char>(0x80 | (code & 0x3F));
  }
}

std::string Trim(const std::string& text) {
  const auto begin = text.find_first_not_of(' ');
  if (begin == std::string::npos) return "";
  const auto end = text.find_last_not_of(' ');
  return text.substr(begin, end - begin + 1);
}

// ---- Scalars and flow collections on one line ----

class InlineParser {
 public:
  InlineParser(const std::string& text, int line) : text_(text), line_(line) {}

  // Parses one value and checks that only spaces or a comment follow.
  Node ParseWhole() {
    Node node = ParseValue(/*in_flow=*/false);
    SkipSpaces();
    if (pos_ < text_.size() && text_[pos_] != '#') {
      throw YamlError(line_, "unexpected text after the value: '" + text_.substr(pos_) + "'");
    }
    return node;
  }

  // Parses a key ending in ':' (followed by a space or the end); returns the key and leaves
  // the position after the ':'. Nothing is consumed when the text is not a key.
  bool TryKey(std::string& key) {
    const std::size_t start = pos_;
    if (pos_ < text_.size() && (text_[pos_] == '"' || text_[pos_] == '\'')) {
      key = text_[pos_] == '"' ? DoubleQuoted() : SingleQuoted();
      SkipSpaces();
      if (pos_ < text_.size() && text_[pos_] == ':' && (pos_ + 1 == text_.size() || text_[pos_ + 1] == ' ')) {
        pos_++;
        return true;
      }
      pos_ = start;
      return false;
    }
    if (pos_ < text_.size() && (text_[pos_] == '[' || text_[pos_] == '{')) return false;
    for (std::size_t i = pos_; i < text_.size(); i++) {
      if (text_[i] == '#' && i > pos_ && text_[i - 1] == ' ') return false;
      if (text_[i] == ':' && (i + 1 == text_.size() || text_[i + 1] == ' ')) {
        const std::string plain = Trim(text_.substr(pos_, i - pos_));
        if (plain.empty()) throw YamlError(line_, "an empty key");
        if (ResolvePlain(plain, line_).kind != Node::Kind::kString) {
          throw YamlError(line_, "the key '" + plain + "' is not a string: quote it");
        }
        key = plain;
        pos_ = i + 1;
        return true;
      }
    }
    return false;
  }

  std::string Rest() {
    SkipSpaces();
    return text_.substr(pos_);
  }

  std::size_t Position() const { return pos_; }

 private:
  void SkipSpaces() {
    while (pos_ < text_.size() && text_[pos_] == ' ') pos_++;
  }

  Node ParseValue(bool in_flow) {
    SkipSpaces();
    if (pos_ >= text_.size()) return Node{};
    const char c = text_[pos_];
    if (c == '"' || c == '\'') {
      Node node;
      node.kind = Node::Kind::kString;
      node.text = c == '"' ? DoubleQuoted() : SingleQuoted();
      return node;
    }
    if (c == '[') return FlowSequence();
    if (c == '{') return FlowMapping();
    // A plain scalar: to the end (or, in a flow collection, to , ] or }), comments cut.
    std::size_t end = pos_;
    while (end < text_.size()) {
      const char d = text_[end];
      if (in_flow && (d == ',' || d == ']' || d == '}' || d == '[' || d == '{')) break;
      if (in_flow && d == ':' && (end + 1 == text_.size() || text_[end + 1] == ' ' || text_[end + 1] == ',')) break;
      if (d == '#' && end > pos_ && text_[end - 1] == ' ') break;
      end++;
    }
    const std::string plain = Trim(text_.substr(pos_, end - pos_));
    pos_ = end;
    return ResolvePlain(plain, line_);
  }

  Node FlowSequence() {
    Node node;
    node.kind = Node::Kind::kSeq;
    pos_++;  // [
    SkipSpaces();
    if (pos_ < text_.size() && text_[pos_] == ']') {
      pos_++;
      return node;
    }
    for (;;) {
      node.items.push_back(ParseValue(/*in_flow=*/true));
      SkipSpaces();
      if (pos_ >= text_.size()) throw YamlError(line_, "a flow sequence must end on its line");
      if (text_[pos_] == ',') {
        pos_++;
        continue;
      }
      if (text_[pos_] == ']') {
        pos_++;
        return node;
      }
      throw YamlError(line_, std::string("unexpected '") + text_[pos_] + "' in a flow sequence");
    }
  }

  Node FlowMapping() {
    Node node;
    node.kind = Node::Kind::kMap;
    pos_++;  // {
    SkipSpaces();
    if (pos_ < text_.size() && text_[pos_] == '}') {
      pos_++;
      return node;
    }
    for (;;) {
      SkipSpaces();
      Node key = ParseValue(/*in_flow=*/true);
      if (key.kind != Node::Kind::kString) throw YamlError(line_, "a flow mapping key must be a string");
      SkipSpaces();
      if (pos_ >= text_.size() || text_[pos_] != ':') throw YamlError(line_, "a flow mapping entry needs ':'");
      pos_++;
      for (const auto& entry : node.entries) {
        if (entry.first == key.text) throw YamlError(line_, "duplicate key '" + key.text + "'");
      }
      node.entries.emplace_back(key.text, ParseValue(/*in_flow=*/true));
      SkipSpaces();
      if (pos_ >= text_.size()) throw YamlError(line_, "a flow mapping must end on its line");
      if (text_[pos_] == ',') {
        pos_++;
        continue;
      }
      if (text_[pos_] == '}') {
        pos_++;
        return node;
      }
      throw YamlError(line_, std::string("unexpected '") + text_[pos_] + "' in a flow mapping");
    }
  }

  std::string SingleQuoted() {
    std::string out;
    pos_++;
    for (;;) {
      if (pos_ >= text_.size()) throw YamlError(line_, "a single-quoted scalar must end on its line");
      const char c = text_[pos_++];
      if (c == '\'') {
        if (pos_ < text_.size() && text_[pos_] == '\'') {
          out += '\'';
          pos_++;
          continue;
        }
        return out;
      }
      out += c;
    }
  }

  std::string DoubleQuoted() {
    std::string out;
    pos_++;
    for (;;) {
      if (pos_ >= text_.size()) throw YamlError(line_, "a double-quoted scalar must end on its line");
      const char c = text_[pos_++];
      if (c == '"') return out;
      if (c != '\\') {
        out += c;
        continue;
      }
      if (pos_ >= text_.size()) throw YamlError(line_, "an escape at the end of the line");
      const char e = text_[pos_++];
      switch (e) {
        case '"': out += '"'; break;
        case '\\': out += '\\'; break;
        case '/': out += '/'; break;
        case 'n': out += '\n'; break;
        case 't': out += '\t'; break;
        case 'r': out += '\r'; break;
        case 'u': {
          if (pos_ + 4 > text_.size()) throw YamlError(line_, "a short \\u escape");
          const uint32_t code = std::stoul(text_.substr(pos_, 4), nullptr, 16);
          if (code >= 0xD800 && code <= 0xDFFF) throw YamlError(line_, "surrogate \\u escapes are not supported");
          AppendUtf8(out, code);
          pos_ += 4;
          break;
        }
        default: throw YamlError(line_, std::string("the escape \\") + e + " is not supported");
      }
    }
  }

  const std::string& text_;
  int line_;
  std::size_t pos_ = 0;
};

// ---- Block structure ----

class BlockParser {
 public:
  explicit BlockParser(std::vector<Line> lines) : lines_(std::move(lines)) {}

  Node ParseDocument() {
    SkipBlank();
    if (AtEnd()) throw YamlError(0, "the document is empty");
    if (lines_[pos_].indent != 0) throw YamlError(lines_[pos_].number, "the document must start at column 0");
    Node root = ParseNode(0);
    SkipBlank();
    if (!AtEnd()) throw YamlError(lines_[pos_].number, "unexpected indentation");
    if (root.kind != Node::Kind::kMap) throw YamlError(1, "the document must be a mapping");
    return root;
  }

 private:
  bool AtEnd() const { return pos_ >= lines_.size(); }

  void SkipBlank() {
    while (!AtEnd() && lines_[pos_].blank) pos_++;
  }

  static bool IsSequenceItem(const std::string& content) {
    return content == "-" || content.starts_with("- ");
  }

  // The node whose first line is the current line, at `indent`.
  Node ParseNode(int indent) {
    return IsSequenceItem(lines_[pos_].content) ? ParseSequence(indent) : ParseMapping(indent);
  }

  // A value after "key:" or "-" with nothing on its own line: the block below, if any.
  Node ParseBlockBelow(int parent_indent, bool allow_same_indent_sequence) {
    SkipBlank();
    if (AtEnd()) return Node{};
    const Line& next = lines_[pos_];
    if (next.indent > parent_indent) return ParseNode(next.indent);
    if (allow_same_indent_sequence && next.indent == parent_indent && IsSequenceItem(next.content)) {
      return ParseSequence(parent_indent);
    }
    return Node{};
  }

  Node ParseValueText(const std::string& rest, int parent_indent, int line_number) {
    if (rest == "|" || rest == "|-") return ParseBlockScalar(parent_indent, rest == "|", line_number);
    if (!rest.empty() && (rest[0] == '|' || rest[0] == '>')) {
      throw YamlError(line_number, "only the `|` and `|-` block scalars are supported");
    }
    if (!rest.empty() && (rest[0] == '&' || rest[0] == '*' || rest[0] == '!')) {
      throw YamlError(line_number, "anchors, aliases and tags are not supported");
    }
    InlineParser inline_parser(rest, line_number);
    return inline_parser.ParseWhole();
  }

  Node ParseMapping(int indent) {
    Node node;
    node.kind = Node::Kind::kMap;
    for (;;) {
      SkipBlank();
      if (AtEnd()) return node;
      Line& line = lines_[pos_];
      if (line.indent < indent) return node;
      if (line.indent > indent) throw YamlError(line.number, "unexpected indentation");
      if (IsSequenceItem(line.content)) return node;  // an indentless sequence's end, for the caller
      InlineParser inline_parser(line.content, line.number);
      std::string key;
      if (!inline_parser.TryKey(key)) throw YamlError(line.number, "expected 'key: value'");
      for (const auto& entry : node.entries) {
        if (entry.first == key) throw YamlError(line.number, "duplicate key '" + key + "'");
      }
      const std::string rest = inline_parser.Rest();
      const int number = line.number;
      pos_++;
      Node value;
      if (rest.empty() || rest[0] == '#') {
        value = ParseBlockBelow(indent, /*allow_same_indent_sequence=*/true);
      } else {
        value = ParseValueText(rest, indent, number);
      }
      node.entries.emplace_back(key, std::move(value));
    }
  }

  Node ParseSequence(int indent) {
    Node node;
    node.kind = Node::Kind::kSeq;
    for (;;) {
      SkipBlank();
      if (AtEnd()) return node;
      Line& line = lines_[pos_];
      if (line.indent < indent) return node;
      if (line.indent > indent) throw YamlError(line.number, "unexpected indentation");
      if (!IsSequenceItem(line.content)) return node;
      std::size_t offset = 1;
      while (offset < line.content.size() && line.content[offset] == ' ') offset++;
      const std::string rest = line.content.substr(offset);
      if (rest.empty() || rest[0] == '#') {
        pos_++;
        node.items.push_back(ParseBlockBelow(indent, /*allow_same_indent_sequence=*/false));
        continue;
      }
      std::string key;
      InlineParser probe(rest, line.number);
      if (IsSequenceItem(rest)) {
        throw YamlError(line.number, "nested sequences on one line are not supported");
      }
      if (probe.TryKey(key)) {
        // "- key: value": a mapping whose first entry is on this line, at the column of `key`.
        line.indent += static_cast<int>(offset);
        line.content = rest;
        node.items.push_back(ParseMapping(line.indent));
        continue;
      }
      const int number = line.number;
      pos_++;
      node.items.push_back(ParseValueText(rest, indent, number));
    }
  }

  Node ParseBlockScalar(int parent_indent, bool keep_final_newline, int line_number) {
    // The block runs while lines are blank or indented more than the parent.
    std::vector<const Line*> body;
    while (!AtEnd() && (lines_[pos_].content.empty() || lines_[pos_].indent > parent_indent)) {
      body.push_back(&lines_[pos_]);
      pos_++;
    }
    // Trailing blank lines belong to what follows, not to a clipped scalar.
    while (!body.empty() && body.back()->content.empty()) {
      body.pop_back();
    }
    if (body.empty()) throw YamlError(line_number, "an empty block scalar");
    const int content_indent = body.front()->indent;
    std::string text;
    for (std::size_t i = 0; i < body.size(); i++) {
      const Line& line = *body[i];
      if (!line.content.empty()) {
        if (line.indent < content_indent) {
          throw YamlError(line.number, "a block scalar line indented less than its first line");
        }
        text += std::string(static_cast<std::size_t>(line.indent - content_indent), ' ');
        text += line.content;
      }
      if (i + 1 < body.size()) text += '\n';
    }
    if (keep_final_newline) text += '\n';
    Node node;
    node.kind = Node::Kind::kString;
    node.text = std::move(text);
    return node;
  }

  std::vector<Line> lines_;
  std::size_t pos_ = 0;
};

std::vector<Line> SplitLines(const std::string& source) {
  std::vector<Line> lines;
  std::istringstream in(source);
  std::string raw;
  int number = 0;
  while (std::getline(in, raw)) {
    number++;
    if (!raw.empty() && raw.back() == '\r') raw.pop_back();
    if (raw.find('\t') != std::string::npos) throw YamlError(number, "tabs are not supported");
    if (number == 1 && raw.starts_with("\xEF\xBB\xBF")) raw.erase(0, 3);
    if (raw.starts_with("---") || raw.starts_with("...") || raw.starts_with("%")) {
      throw YamlError(number, "document markers and directives are not supported");
    }
    Line line;
    line.number = number;
    const auto first = raw.find_first_not_of(' ');
    if (first == std::string::npos) {
      line.blank = true;
    } else {
      line.indent = static_cast<int>(first);
      line.content = raw.substr(first);
      while (!line.content.empty() && line.content.back() == ' ') line.content.pop_back();
      line.blank = line.content[0] == '#';
    }
    lines.push_back(std::move(line));
  }
  return lines;
}

// ---- JSON ----

void WriteString(std::string& out, const std::string& text) {
  out += '"';
  for (const unsigned char c : text) {
    switch (c) {
      case '"': out += "\\\""; break;
      case '\\': out += "\\\\"; break;
      case '\n': out += "\\n"; break;
      case '\r': out += "\\r"; break;
      case '\t': out += "\\t"; break;
      default:
        if (c < 0x20) {
          char escaped[8];
          std::snprintf(escaped, sizeof escaped, "\\u%04x", c);
          out += escaped;
        } else {
          out += static_cast<char>(c);
        }
    }
  }
  out += '"';
}

void WriteJson(std::string& out, const Node& node) {
  switch (node.kind) {
    case Node::Kind::kNull: out += "null"; break;
    case Node::Kind::kBool: out += node.boolean ? "true" : "false"; break;
    case Node::Kind::kInt: out += node.text; break;
    case Node::Kind::kString: WriteString(out, node.text); break;
    case Node::Kind::kSeq:
      out += '[';
      for (std::size_t i = 0; i < node.items.size(); i++) {
        if (i) out += ',';
        WriteJson(out, node.items[i]);
      }
      out += ']';
      break;
    case Node::Kind::kMap:
      out += '{';
      for (std::size_t i = 0; i < node.entries.size(); i++) {
        if (i) out += ',';
        WriteString(out, node.entries[i].first);
        out += ':';
        WriteJson(out, node.entries[i].second);
      }
      out += '}';
      break;
  }
}

uint64_t Fnv1a(const std::string& text) {
  uint64_t hash = 0xcbf29ce484222325ULL;
  for (const unsigned char c : text) {
    hash ^= c;
    hash *= 0x100000001b3ULL;
  }
  return hash;
}

std::string CppSource(const std::string& json, const std::string& source_name) {
  std::string out;
  out += "// Generated from " + source_name + " by cpp/tools/openapi_json.cc. Do not edit.\n";
  out += "#include \"openapi_document.hpp\"\n\nnamespace warpsql {\nnamespace {\n\n";
  // A byte array, not a string literal: compilers cap the length of a literal.
  out += "constexpr unsigned char kJson[] = {";
  for (std::size_t i = 0; i < json.size(); i++) {
    if (i % 20 == 0) out += "\n   ";
    char byte[8];
    std::snprintf(byte, sizeof byte, " %u,", static_cast<unsigned char>(json[i]));
    out += byte;
  }
  out += "\n};\n\n}  // namespace\n\n";
  out += "std::string_view OpenApiJson() {\n"
         "  return {reinterpret_cast<const char*>(kJson), sizeof kJson};\n}\n\n";
  char etag[40];
  std::snprintf(etag, sizeof etag, "\"%016llx\"", static_cast<unsigned long long>(Fnv1a(json)));
  out += "std::string_view OpenApiEtag() { return R\"(" + std::string(etag) + ")\"; }\n\n";
  out += "}  // namespace warpsql\n";
  return out;
}

bool Write(const std::string& path, const std::string& content) {
  std::ofstream out(path, std::ios::binary | std::ios::trunc);
  out << content;
  return static_cast<bool>(out);
}

}  // namespace

int main(int argc, char** argv) {
  if (argc != 3 && argc != 4) {
    std::cerr << "usage: openapi_json <openapi.yaml> <out.cc> [<out.json>]\n";
    return 2;
  }
  std::ifstream in(argv[1], std::ios::binary);
  if (!in) {
    std::cerr << "openapi_json: cannot read " << argv[1] << "\n";
    return 1;
  }
  std::stringstream source;
  source << in.rdbuf();
  try {
    BlockParser parser(SplitLines(source.str()));
    const Node root = parser.ParseDocument();
    std::string json;
    WriteJson(json, root);
    if (!Write(argv[2], CppSource(json, "spec/openapi.yaml")) ||
        (argc == 4 && !Write(argv[3], json + "\n"))) {
      std::cerr << "openapi_json: cannot write the output\n";
      return 1;
    }
  } catch (const std::exception& error) {
    std::cerr << argv[1] << ": " << error.what() << "\n";
    return 1;
  }
  return 0;
}
