import {
  GunGetOpts,
  GunGraphAdapter,
  GunGraphData,
  GunNode
} from '@chaingun/types'
import uuid from 'uuid'

type PeerSet = Record<string, GunGraphAdapter>

export interface FederatedAdapterOpts {
  readonly maxStaleness?: number
  readonly putToPeers?: boolean
  readonly maintainChangelog?: boolean
}

const CHANGELOG_SOUL = 'changelog'
const PEER_SYNC_SOUL = `peersync`

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

type ChangeSetEntry = readonly [string, GunGraphData]

export function getChangesetFeed(
  peer: GunGraphAdapter,
  from: string
): () => Promise<ChangeSetEntry | null> {
  // tslint:disable-next-line: no-let
  let lastKey = from
  // tslint:disable-next-line: readonly-array
  const changes: ChangeSetEntry[] = []
  // tslint:disable-next-line: no-let
  let nodePromise: Promise<GunNode | null> | null = null

  return async function getNext(): Promise<
    readonly [string, GunGraphData] | null
  > {
    if (!changes.length && !nodePromise) {
      nodePromise = peer.get(CHANGELOG_SOUL, {
        '>': `${lastKey}一`
      })
      const node = await nodePromise

      if (node) {
        for (const key in node) {
          if (key && key !== '_') {
            changes.splice(0, 0, [key, node[key]])
            lastKey = key
          }
        }
      }
    } else if (nodePromise) {
      await nodePromise
    }

    const entry = changes.pop()
    return entry || null
  }
}

export async function syncWithPeer(
  internal: GunGraphAdapter,
  persist: GunGraphAdapter,
  name: string,
  peer: GunGraphAdapter,
  from: string
): Promise<string> {
  const getNext = getChangesetFeed(peer, from)
  // tslint:disable-next-line: no-let
  let lastKey: string = ''
  // tslint:disable-next-line: no-let
  let entry: ChangeSetEntry | null

  // tslint:disable-next-line: no-conditional-assignment
  while ((entry = await getNext())) {
    const [key, changes] = entry
    await persist.put(changes)
    await internal.put({
      [PEER_SYNC_SOUL]: {
        _: {
          '#': PEER_SYNC_SOUL,
          '>': {
            [name]: new Date().getTime()
          }
        },
        [name]: key
      }
    })

    lastKey = key
  }

  return lastKey
}

export async function syncWithPeers(
  internal: GunGraphAdapter,
  persist: GunGraphAdapter,
  allPeers: PeerSet
): Promise<void> {
  const entries = Object.entries(allPeers)
  const yesterday = new Date(Date.now() - 24 * 3600 * 1000).toISOString()
  return entries.length
    ? Promise.all(
        entries.map(async ([name, peer]) => {
          const node = await internal.get(PEER_SYNC_SOUL, { '.': name })
          const key = (node && node[name]) || yesterday
          return syncWithPeer(internal, persist, name, peer, key)
        })
      ).then(NOOP)
    : Promise.resolve()
}

export interface FederatedGunGraphAdapter extends GunGraphAdapter {
  readonly syncWithPeers: () => Promise<void>
  readonly getChangesetFeed: (
    from: string
  ) => () => Promise<ChangeSetEntry | null>
}

export function createFederatedAdapter(
  internal: GunGraphAdapter,
  external: PeerSet,
  persistence?: GunGraphAdapter,
  adapterOpts: FederatedAdapterOpts = DEFAULTS
): FederatedGunGraphAdapter {
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
    },

    syncWithPeers: () => syncWithPeers(internal, persist, external),

    getChangesetFeed: (from: string) => getChangesetFeed(internal, from)
  }
}

export const FederationAdapter = {
  create: createFederatedAdapter
}
