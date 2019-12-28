import { GunGetOpts, GunGraphAdapter, GunGraphData } from '@chaingun/types'
import uuid from 'uuid'

type PeerSet = Record<string, GunGraphAdapter>

export interface FederatedAdapterOpts {
  readonly maxStaleness?: number
  readonly putToPeers?: boolean
  readonly maintainChangelog?: boolean
}

const CHANGELOG_SOUL = 'changelog'

const DEFAULTS = {
  maintainChangelog: true,
  maxStaleness: 1000 * 60 * 60 * 24,
  putToPeers: false
}

const NOOP = () => {
  // intentionally left blank
}

async function updateChangelog(
  internal: GunGraphAdapter,
  diff: GunGraphData
): Promise<void> {
  const now = new Date()
  const itemKey = `${now.toISOString()}-${uuid.v4()}`

  await internal.put({
    [CHANGELOG_SOUL]: {
      _: {
        '#': CHANGELOG_SOUL,
        '>': {
          [itemKey]: now.getTime()
        }
      },
      [itemKey]: diff
    }
  })
}

async function updateFromPeer(
  internal: GunGraphAdapter,
  persist: GunGraphAdapter,
  name: string,
  peer: GunGraphAdapter,
  soul: string,
  opts?: FederatedAdapterOpts
): Promise<void> {
  const {
    maxStaleness = DEFAULTS.maxStaleness,
    maintainChangelog = DEFAULTS.maintainChangelog
  } = opts || DEFAULTS
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
    const diff = await persist.put({
      [soul]: node
    })

    if (diff && maintainChangelog) {
      updateChangelog(internal, diff)
    }
  }

  await internal.put({
    [peerSoul]: {
      _: {
        '#': peerSoul,
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
  opts?: FederatedAdapterOpts
): Promise<void> {
  const entries = Object.entries(allPeers)
  return entries.length
    ? Promise.all(
        entries.map(([name, peer]) =>
          updateFromPeer(internal, persist, name, peer, soul, opts)
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

export function createFederatedAdapter(
  internal: GunGraphAdapter,
  external: PeerSet,
  persistence?: GunGraphAdapter,
  adapterOpts: FederatedAdapterOpts = DEFAULTS
): GunGraphAdapter {
  const {
    putToPeers = DEFAULTS.putToPeers,
    maintainChangelog = DEFAULTS.maintainChangelog
  } = adapterOpts
  const persist = persistence || internal
  const peers = { ...external }

  return {
    get: async (soul: string, opts?: GunGetOpts) => {
      await updateFromPeers(internal, persist, peers, soul, adapterOpts)
      return internal.get(soul, opts)
    },

    getJsonString: internal.getJsonString
      ? async (soul: string, opts?: GunGetOpts) => {
          await updateFromPeers(internal, persist, peers, soul, adapterOpts)
          return internal.getJsonString!(soul, opts)
        }
      : undefined,

    put: async (data: GunGraphData) => {
      const diff = await persist.put(data)

      if (!diff) {
        return diff
      }

      if (maintainChangelog) {
        updateChangelog(internal, diff)
      }

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
