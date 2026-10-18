import assert from "node:assert/strict";
import { stat, symlink } from "node:fs/promises";
import type { TestContext } from "node:test";

// Windows stat exposes a read-only attribute, not owner/group ACLs.
export async function assertMode(path: string, expected: number): Promise<void> {
  const actual = (await stat(path)).mode & 0o777;
  if (process.platform === "win32") assert.equal((actual & 0o200) !== 0, (expected & 0o200) !== 0);
  else assert.equal(actual, expected);
}

export async function symlinkOrSkip(context: TestContext, target: string, path: string): Promise<boolean> {
  try { await symlink(target, path); return true; }
  catch (error) {
    if (process.platform === "win32" && (error as NodeJS.ErrnoException).code === "EPERM") {
      context.skip("Windows requires Developer Mode or symlink privilege for this fixture");
      return false;
    }
    throw error;
  }
}
