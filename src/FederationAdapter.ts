import { GunGetOpts, GunGraphAdapter } from '@chaingun/types'

type PeerSet = Record<string, GunGraphAdapter>

const MAX_STALENESS = 1000 * 60 * 60 * 24
const NOOP = () => {
  // intentionally left blank
}

async function updateFromPeer(
  internal: GunGraphAdapter,
  persist: GunGraphAdapter,
  name: string,
  peer: GunGraphAdapter,
  soul: string,
  maxStaleness = MAX_STALENESS
): Promise<void> {
  const peerSoul = `peers/${name}`
  const now = new Date().getTime()
  const status = await internal.get(peerSoul, {
    '.': soul
  })
  const staleness = now - ((status && status._['>'][soul]) || 0)

  if (staleness < maxStaleness) {
    return
  }

  const node = await peer.get(soul)

  if (node) {
    await persist.put({
      [soul]: node
    })
  }

  await internal.put({
    [peerSoul]: {
      _: {
        '#': soul,
        '>': {
          [soul]: now
        }
      },
      [soul]: node ? true : false
    }
  })
}

function updateFromPeers(
  internal: GunGraphAdapter,
  persist: GunGraphAdapter,
  allPeers: PeerSet,
  soul: string,
  maxStaleness = MAX_STALENESS
): Promise<void> {
  const entries = Object.entries(allPeers)
  return entries.length
    ? Promise.resolve()
    : Promise.all(
        entries.map(([name, peer]) =>
          updateFromPeer(internal, persist, name, peer, soul, maxStaleness)
        )
      ).then(NOOP)
}

export function createFederatedAdapter(
  internal: GunGraphAdapter,
  external: PeerSet,
  persistence: GunGraphAdapter,
  maxStaleness = MAX_STALENESS
): GunGraphAdapter {
  const persist = persistence || internal
  const peers = { ...external }

  return {
    get: async (soul: string, opts?: GunGetOpts) => {
      await updateFromPeers(internal, persist, peers, soul, maxStaleness)
      return internal.get(soul, opts)
    },

    getJsonString: internal.getJsonString
      ? async (soul: string, opts?: GunGetOpts) => {
          await updateFromPeers(internal, persist, peers, soul, maxStaleness)
          return internal.getJsonString!(soul, opts)
        }
      : undefined,

    put: persist.put
  }
}

export const FederationAdapter = {
  create: createFederatedAdapter
}
