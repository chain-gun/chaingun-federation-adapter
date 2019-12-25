import { GunGetOpts, GunGraphAdapter, GunGraphData } from '@chaingun/types'

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
    ? Promise.all(
        entries.map(([name, peer]) =>
          updateFromPeer(internal, persist, name, peer, soul, maxStaleness)
        )
      ).then(NOOP)
    : Promise.resolve()
}

function updatePeers(data: GunGraphData, allPeers: PeerSet): Promise<void> {
  const entries = Object.entries(allPeers)
  return entries.length
    ? Promise.all(
        entries.map(([name, peer]) =>
          peer.put(data).catch(err => {
            // @ts-ignore
            // tslint:disable-next-line: no-console
            console.warn('Failed to update peer', name, err.stack || err, data)
          })
        )
      ).then(NOOP)
    : Promise.resolve()
}

export interface FederatedAdapterOpts {
  readonly maxStaleness?: number
  readonly putToPeers?: boolean
}

export function createFederatedAdapter(
  internal: GunGraphAdapter,
  external: PeerSet,
  persistence?: GunGraphAdapter,
  adapterOpts: FederatedAdapterOpts = {}
): GunGraphAdapter {
  const { maxStaleness = MAX_STALENESS, putToPeers = false } = adapterOpts
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

    put: async (data: GunGraphData) => {
      const diff = await persist.put(data)

      if (diff && putToPeers) {
        updatePeers(diff, peers)
      }

      return diff
    }
  }
}

export const FederationAdapter = {
  create: createFederatedAdapter
}
