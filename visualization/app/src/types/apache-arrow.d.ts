declare module "apache-arrow" {
  export type Table = any;

  export function tableFromIPC(bytes: Uint8Array | ArrayBuffer): Table;
  export function tableToIPC(table: any, options?: any): Uint8Array;
  export function tableFromArrays(arrays: Record<string, any>): Table;
}

