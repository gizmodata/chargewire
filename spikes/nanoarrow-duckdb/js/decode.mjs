// Reads an Arrow IPC stream the way a browser client would, touches every value,
// and writes the table back out so the harness can compare it with the original.
//   node decode.mjs <in.arrows> <out.arrows>
import { readFileSync, writeFileSync } from "node:fs";
import { tableFromIPC, tableToIPC } from "apache-arrow";

const [input, output] = process.argv.slice(2);
const table = tableFromIPC(readFileSync(input));
for (const field of table.schema.fields) {
  const column = table.getChild(field.name);
  for (let i = 0; i < column.length; i++) {
    column.get(i);
  }
}
writeFileSync(output, tableToIPC(table, "stream"));
