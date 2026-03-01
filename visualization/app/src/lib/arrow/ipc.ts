import { tableFromIPC, type Table } from "apache-arrow";

export function decodeArrowIpcStream(bytes: Uint8Array): Table {
  return tableFromIPC(bytes);
}
