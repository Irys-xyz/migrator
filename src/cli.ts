import Query from "@irys/query/node";
import { NodeIrys } from "@irys/sdk/build/cjs/node/index";
import PromisePool from "@supercharge/promise-pool";
import { Command } from "commander";
import { hash } from "crypto";
import { PathLike, promises as fsPromises } from "fs";
import { resolve, dirname } from "path";
import { Pool } from "undici";
export const cli = new Command();

const testnetBundlerUrl = "https://testnet-upload.irys.xyz";
const arweaveBundlerUrl = "https://node1.irys.xyz";
cli
  .option(
    "-a, --addresses <string...>",
    "list of addresses, or a path to a file containing newline delimited addresses. all tag filters are applied to all supplied addresses.",
  )
  .option(
    "-t, --tags <string...>",
    "list of tag (name|value) pairs, or a path to a file containing newline delimited pairs. Tags are applied as AND filters. multiple values for the same name is supported.",
  )
  .option(
    "--concurrency <number>",
    "number of parallel migrations to perform - reduce this if you get timeout errors.",
    "20",
  )
  .option(
    "--checkpoint-size <number>",
    "number of items to migrate before checkpointing.",
    "200",
  );

cli.command("migrate").action(async () => {
  // parse address and tag options
  const addresses = await parseAddressesArg(options.addresses);
  const tags = await parseTagsArg(options.tags);

  const query = new Query({
    url: new URL(`${arweaveBundlerUrl}/graphql`),
  }).search("irys:transactions");

  const checkpoint = await getProgress(addresses, tags);
  if (checkpoint)
    console.debug(`resuming from checkpoint cursor ${checkpoint}`);
  const q2 = query
    .fields({
      id: true,
      token: true,
    })
    .pageSize(+(options.checkpointSize ?? 200))
    .limit(Infinity)
    .from(addresses)
    .tags(tags)
    .sort("DESC")
    .after(checkpoint);

  let page = await q2.getPage();
  const node1Pool = ConnectionPool(arweaveBundlerUrl);
  const testnetUploadPool = ConnectionPool(testnetBundlerUrl);
  while (page?.length) {
    const pagePromise = q2.getPage();
    const page2 = page.filter(Boolean);
    const p = PromisePool.for(page2)
      .withConcurrency(+(options.concurrency ?? 20))
      .process(async (fields) => {
        if (!fields?.id) return;
        return await migrate({
          txId: fields.id,
          token: fields.token,
          node1Pool,
          testnetUploadPool,
        });
      });

    let { errors } = await p;
    if (errors.length != 0) {
      errors.forEach((e) => console.error(e));
      throw new Error("Unexpected Errors while migrating - aborting");
    }
    await saveProgress(addresses, tags, query.queryVars.after);
    // defer await to give it more time to fetch the page
    page = await pagePromise;
  }
  console.log("Migration complete");
});

export const ConnectionPool = (url: string): Pool =>
  new Pool(url, {
    headersTimeout: 10_000,
    bodyTimeout: 20_000,
    keepAliveTimeout: 10_000,
    pipelining: 10,
    maxRequestsPerClient: 1000,
  });

const irysInstanceMap = new Map<string, NodeIrys>();

async function migrate({
  txId,
  token,
  node1Pool,
  testnetUploadPool,
}: {
  txId: string;
  token: string;
  node1Pool: Pool;
  testnetUploadPool: Pool;
}) {
  // check that the testnet-bundler doesn't have this tx
  const res = await testnetUploadPool.request({
    path: `/tx/${txId}/data`,
    method: "HEAD",
  });
  // if it does, do an early return.
  if (res.statusCode === 200 && res.headers["x-content-length"]) {
    return console.debug(`skipping ${txId} as it's already migrated`);
  }

  // connect to node1, pull data as a stream
  const stream = await node1Pool.request({
    path: `/tx/${txId}/raw`,
    method: "GET",
    highWaterMark: 2 ** 18,
  });

  const irys = getIrys(token);
  // todo: use connection pool for uploading

  const uploader = irys.uploader.chunkedUploader;
  const uploadRes = await uploader.uploadTransaction(stream.body);
  console.debug(`Migrated ${txId}`);
  return uploadRes;
}

async function saveProgress(
  address: string,
  tags: { name: string; values: string[] }[],
  latestTxId: string,
) {
  const name = getCheckpointName(address, tags);
  const bn = dirname(name);
  await fsPromises.mkdir(bn, { recursive: true });
  await fsPromises.writeFile(name, address + tags + "\n" + latestTxId);
}

async function getProgress(
  addresses: string[],
  tags: { name: string; values: string[] }[],
) {
  const name = getCheckpointName(addresses, tags);
  return (await checkPath(name))
    ? (await fsPromises.readFile(name)).toString().split("\n")[1]
    : undefined;
}

const getCheckpointName = (addresses, tags) =>
  resolve(
    "./.checkpoints",
    hash("sha256", addresses.toString() + tags.toString()),
  );

function getIrys(token: string) {
  const get = irysInstanceMap.get(token);
  if (get) return get;
  const irys = new NodeIrys({ url: testnetBundlerUrl, token });
  irysInstanceMap.set(token, irys);
  return irys;
}

async function parseAddressesArg(addressArg) {
  if (!addressArg || addressArg?.length == 0) return;
  const addresses = (await checkPath(addressArg))
    ? readFile(addressArg)
    : addressArg;
  return addresses;
}

async function readFile(path) {
  // expect data to be "\n" delimited
  const data = (await fsPromises.readFile(path)).toString().split("\n");
  return data;
}

async function parseTagsArg(
  tagsArg,
): Promise<{ name: string; values: string[] }[]> {
  if (!tagsArg || tagsArg?.length == 0) return [];
  const rawTags: string[] = (await checkPath(tagsArg))
    ? (await readFile(tagsArg)).toString().split("\n")
    : tagsArg;
  const tags: { [key: string]: string[] } = rawTags.reduce((acc, t) => {
    const [n, v] = t.split("|");
    acc[n] ? acc[n].push(v) : (acc[n] = [v]);
    return acc;
  }, {});

  const tags2 = Object.entries(tags)
    .sort(([k, _], [k2, _2]) => (k > k2 ? 1 : k == k2 ? 0 : -1))
    .map(([k, v]) => ({ name: k, values: v }));
  return tags2;
}

export const checkPath = async (path: PathLike): Promise<boolean> => {
  return fsPromises
    .stat(path)
    .then((_) => true)
    .catch((_) => false);
};

const options = cli.opts();

const isScript = require.main === module;
if (isScript) {
  const argv = process.argv;

  // const argv = [
  //   "aaa",
  //   "aaa",
  //   "migrate",
  //   "-a",
  //   "7smNXWVNbTinRPuKbrke0XR0N9N6FgTBVCh20niXEbU",
  //   // "-t",
  //   // // "content-type|text/plain",
  //   // "Content-type|text/plain",
  //   // "content-type|text/plain2",
  // ];
  cli.parse(argv);
}
