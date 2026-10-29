// chargewire-spike: moves Arrow data between DuckDB and Arrow IPC streams the way a Chargewire
// SQL server would, so a test harness can check every type survives.
//
//   chargewire-spike export --db PATH --sql SQL --out FILE [--compression none|zstd|lz4]
//                     [--batch-rows N] [--setup SQL]
//   chargewire-spike ingest --db PATH --table NAME --in FILE
//                     [--mode create|append|replace|create_append] [--setup SQL]
//   chargewire-spike copy   --in FILE --out FILE [--compression none|zstd|lz4]

#include <cstdio>
#include <iostream>
#include <map>
#include <memory>
#include <string>

#include "database.hpp"
#include "ipc.hpp"

namespace {

using Args = std::map<std::string, std::string>;

struct FileCloser {
  void operator()(std::FILE* file) const { std::fclose(file); }
};
using File = std::unique_ptr<std::FILE, FileCloser>;

File Open(const std::string& path, const char* mode) {
  File file(std::fopen(path.c_str(), mode));
  if (!file) {
    throw chargewire::Error("cannot open " + path);
  }
  return file;
}

Args ParseArgs(int argc, char** argv) {
  Args args;
  for (int i = 2; i < argc; i++) {
    std::string key = argv[i];
    if (key.rfind("--", 0) != 0 || i + 1 >= argc) {
      throw chargewire::Error("expected --name value pairs, got '" + key + "'");
    }
    args[key.substr(2)] = argv[++i];
  }
  return args;
}

const std::string& Required(const Args& args, const std::string& name) {
  auto it = args.find(name);
  if (it == args.end()) {
    throw chargewire::Error("missing --" + name);
  }
  return it->second;
}

std::string Optional(const Args& args, const std::string& name, const std::string& fallback) {
  auto it = args.find(name);
  return it == args.end() ? fallback : it->second;
}

chargewire::IngestMode ParseMode(const std::string& name) {
  if (name == "create") return chargewire::IngestMode::kCreate;
  if (name == "append") return chargewire::IngestMode::kAppend;
  if (name == "replace") return chargewire::IngestMode::kReplace;
  if (name == "create_append") return chargewire::IngestMode::kCreateAppend;
  throw chargewire::Error("unknown mode '" + name + "'");
}

void Export(const Args& args) {
  chargewire::Database db(Required(args, "db"));
  if (auto setup = Optional(args, "setup", ""); !setup.empty()) {
    db.Execute(setup);
  }
  auto rows = db.Query(Required(args, "sql"), std::stoll(Optional(args, "batch-rows", "0")));
  File out = Open(Required(args, "out"), "wb");
  chargewire::WriteIpcStream(rows.get(), out.get(),
                       chargewire::ParseCompression(Optional(args, "compression", "none")));
}

void Ingest(const Args& args) {
  chargewire::Database db(Required(args, "db"));
  if (auto setup = Optional(args, "setup", ""); !setup.empty()) {
    db.Execute(setup);
  }
  File in = Open(Required(args, "in"), "rb");
  auto rows = chargewire::ReadIpcStream(in.get());
  const int64_t count = db.Ingest(Required(args, "table"),
                                  ParseMode(Optional(args, "mode", "create_append")), rows.get());
  std::cout << count << "\n";
}

void Copy(const Args& args) {
  File in = Open(Required(args, "in"), "rb");
  auto rows = chargewire::ReadIpcStream(in.get());
  File out = Open(Required(args, "out"), "wb");
  chargewire::WriteIpcStream(rows.get(), out.get(),
                       chargewire::ParseCompression(Optional(args, "compression", "none")));
}

}  // namespace

int main(int argc, char** argv) {
  if (argc < 2) {
    std::cerr << "usage: chargewire-spike export|ingest|copy --name value ...\n";
    return 2;
  }
  const std::string command = argv[1];
  try {
    const Args args = ParseArgs(argc, argv);
    if (command == "export") {
      Export(args);
    } else if (command == "ingest") {
      Ingest(args);
    } else if (command == "copy") {
      Copy(args);
    } else {
      throw chargewire::Error("unknown command '" + command + "'");
    }
  } catch (const std::exception& e) {
    std::cerr << "chargewire-spike " << command << ": " << e.what() << "\n";
    return 1;
  }
  return 0;
}
