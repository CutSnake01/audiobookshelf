# GPodder Sync & Remote Streaming Implementation Plan

## Executive Summary

This document outlines the implementation of two interconnected features:
1. **Remote Episode Streaming** - Play podcast episodes directly from feed URLs without downloading
2. **GPodder Sync** - Synchronize subscriptions and listening progress with Nextcloud GPodder

These features are tightly coupled: GPodder sync is only useful if Audiobookshelf can track progress on episodes that haven't been downloaded locally.

---

## Part 1: Remote Episode Streaming

### Overview

Currently, Audiobookshelf requires podcast episodes to be downloaded locally before playback. This change allows episodes to be tracked in the database and played directly from their `enclosureURL`.

### Architecture Changes

#### 1.1 Database Schema Changes

**New Table: `virtualEpisodes`** (tracks episodes not downloaded locally)

```sql
CREATE TABLE virtualEpisodes (
  id UUID PRIMARY KEY DEFAULT uuid_v4(),
  podcastId UUID NOT NULL REFERENCES podcasts(id) ON DELETE CASCADE,

  -- Episode identification (from RSS feed)
  guid TEXT,
  enclosureURL TEXT NOT NULL,
  enclosureSize BIGINT,
  enclosureType TEXT,

  -- Metadata
  title TEXT NOT NULL,
  subtitle TEXT,
  description TEXT,
  pubDate TEXT,
  publishedAt DATETIME,
  season TEXT,
  episode TEXT,
  episodeType TEXT,
  duration INTEGER,  -- Estimated from RSS, updated after first play

  -- Sync tracking
  source TEXT DEFAULT 'gpodder',  -- 'gpodder', 'manual', 'feed'
  lastSyncedAt DATETIME,

  createdAt DATETIME NOT NULL,
  updatedAt DATETIME NOT NULL,

  UNIQUE(podcastId, guid),
  UNIQUE(podcastId, enclosureURL)
);

CREATE INDEX idx_virtual_episodes_podcast ON virtualEpisodes(podcastId);
CREATE INDEX idx_virtual_episodes_guid ON virtualEpisodes(guid);
```

**Alternative Approach: Extend `podcastEpisodes` Table**

Instead of a new table, make `audioFile` nullable:

```javascript
// Migration: v2.X.X-optional-audiofile.js
async function up({ context: { queryInterface, logger } }) {
  // Allow audioFile to be NULL for virtual episodes
  await queryInterface.changeColumn('podcastEpisodes', 'audioFile', {
    type: DataTypes.JSON,
    allowNull: true  // Changed from NOT NULL
  });

  // Add column to track if episode is virtual
  await queryInterface.addColumn('podcastEpisodes', 'isVirtual', {
    type: DataTypes.BOOLEAN,
    defaultValue: false
  });
}
```

**Recommendation**: Use the second approach (extend existing table) for simpler queries and consistent episode handling.

#### 1.2 Model Changes

**`server/models/PodcastEpisode.js`**

```javascript
// Add new property
/** @type {boolean} */
this.isVirtual  // true if no local audioFile

// Modify getAudioTrack method
getAudioTrack(libraryItemId) {
  if (this.audioFile) {
    // Existing behavior: local file
    const track = structuredClone(this.audioFile)
    track.startOffset = 0
    track.title = this.audioFile.metadata.filename
    track.index = 1
    track.contentUrl = `/api/items/${libraryItemId}/file/${track.ino}`
    return track
  } else if (this.enclosureURL) {
    // New behavior: remote stream
    return {
      index: 1,
      startOffset: 0,
      duration: this.duration || 0,
      title: this.title,
      contentUrl: `/api/items/${libraryItemId}/episode/${this.id}/stream`,
      mimeType: this.enclosureType || 'audio/mpeg',
      isRemote: true,
      metadata: {
        filename: this.title,
        ext: this.getExtensionFromEnclosure(),
        size: this.enclosureSize
      }
    }
  }
  return null
}

// Helper method
getExtensionFromEnclosure() {
  if (!this.enclosureURL) return '.mp3'
  const url = new URL(this.enclosureURL)
  const ext = Path.extname(url.pathname)
  return ext || '.mp3'
}
```

**`server/models/Podcast.js`**

```javascript
// Modify checkCanDirectPlay to handle virtual episodes
checkCanDirectPlay(supportedMimeTypes, episodeId) {
  const episode = this.podcastEpisodes.find(ep => ep.id === episodeId)
  if (!episode) return false

  if (episode.isVirtual) {
    // Virtual episodes always "direct play" via proxy stream
    return true
  }

  // Existing logic for local files...
}
```

#### 1.3 New Streaming Proxy Endpoint

**`server/controllers/PodcastController.js`** - New endpoint

```javascript
/**
 * GET: /api/items/:id/episode/:episodeId/stream
 * Proxy stream for remote podcast episodes
 */
async streamRemoteEpisode(req, res) {
  const { libraryItem } = req
  const { episodeId } = req.params

  const episode = libraryItem.media.podcastEpisodes.find(ep => ep.id === episodeId)
  if (!episode) {
    return res.status(404).send('Episode not found')
  }

  // If episode has local file, redirect to file endpoint
  if (episode.audioFile) {
    return res.redirect(`/api/items/${libraryItem.id}/file/${episode.audioFile.ino}`)
  }

  if (!episode.enclosureURL) {
    return res.status(404).send('Episode has no audio source')
  }

  try {
    // Validate URL (SSRF protection)
    const url = new URL(episode.enclosureURL)
    if (!['http:', 'https:'].includes(url.protocol)) {
      return res.status(400).send('Invalid URL protocol')
    }

    // Stream with range request support
    const headers = {
      'User-Agent': 'audiobookshelf (+https://audiobookshelf.org)'
    }

    // Forward range headers for seeking
    if (req.headers.range) {
      headers['Range'] = req.headers.range
    }

    const response = await axios({
      method: 'GET',
      url: episode.enclosureURL,
      responseType: 'stream',
      headers,
      timeout: 30000,
      httpAgent: ssrfFilter(episode.enclosureURL),
      httpsAgent: ssrfFilter(episode.enclosureURL)
    })

    // Forward relevant headers
    if (response.headers['content-type']) {
      res.setHeader('Content-Type', response.headers['content-type'])
    }
    if (response.headers['content-length']) {
      res.setHeader('Content-Length', response.headers['content-length'])
    }
    if (response.headers['content-range']) {
      res.setHeader('Content-Range', response.headers['content-range'])
    }
    if (response.headers['accept-ranges']) {
      res.setHeader('Accept-Ranges', response.headers['accept-ranges'])
    }

    // Set status (206 for partial content, 200 otherwise)
    res.status(response.status)

    // Pipe the stream
    response.data.pipe(res)

    // Handle client disconnect
    req.on('close', () => {
      response.data.destroy()
    })

  } catch (error) {
    Logger.error(`[PodcastController] Failed to stream remote episode: ${error.message}`)
    if (!res.headersSent) {
      res.status(502).send('Failed to fetch remote audio')
    }
  }
}
```

#### 1.4 Route Registration

**`server/routers/ApiRouter.js`**

```javascript
// Add new route (must be before /:id/episode/:episodeId)
this.router.get('/items/:id/episode/:episodeId/stream',
  LibraryItemController.middleware.bind(this),
  PodcastController.streamRemoteEpisode.bind(this)
)
```

#### 1.5 PlaybackSessionManager Changes

**`server/managers/PlaybackSessionManager.js`**

```javascript
async startSession(user, deviceInfo, libraryItem, episodeId, options) {
  // ... existing code ...

  const episode = episodeId ? libraryItem.media.podcastEpisodes.find(ep => ep.id === episodeId) : null

  // Handle virtual episodes
  if (episode?.isVirtual) {
    // Virtual episodes always use the proxy stream
    const audioTrack = episode.getAudioTrack(libraryItem.id)
    audioTracks = [audioTrack]
    newPlaybackSession.playMethod = PlayMethod.DIRECTPLAY

    // Update duration if we got it from headers (will be done in sync)
  } else {
    // Existing logic for local files...
  }

  // ... rest of existing code ...
}
```

#### 1.6 Client-Side Changes

**`client/players/AudioTrack.js`**

```javascript
constructor(track, sessionId, startOffset) {
  this.index = track.index
  this.startOffset = startOffset
  this.duration = track.duration
  this.title = track.title
  this.contentUrl = track.contentUrl || null
  this.mimeType = track.mimeType
  this.isRemote = track.isRemote || false  // New property

  // Determine the session track URL
  if (this.contentUrl?.startsWith('/hls')) {
    this.sessionTrackUrl = this.contentUrl
  } else if (this.isRemote) {
    // Remote streams use contentUrl directly (already includes /stream endpoint)
    this.sessionTrackUrl = this.contentUrl
  } else {
    this.sessionTrackUrl = `/public/session/${sessionId}/track/${this.index}`
  }
}
```

---

## Part 2: GPodder Sync Integration

### Overview

Implement the GPodder API to sync with Nextcloud GPodder servers, allowing:
- Subscription sync (add/remove podcasts)
- Episode action sync (play progress, finished state)

### 2.1 User Configuration

**Add to `server/models/User.js` extraData schema:**

```javascript
extraData: {
  // ... existing fields ...
  gpodder: {
    enabled: false,
    serverUrl: null,        // e.g., 'https://nextcloud.example.com'
    username: null,
    password: null,         // Encrypted
    deviceId: 'audiobookshelf',
    lastSubscriptionSync: null,
    lastEpisodeActionSync: null,
    syncInterval: 30        // minutes
  }
}
```

### 2.2 New GPodder Manager

**`server/managers/GPodderSyncManager.js`**

```javascript
const axios = require('axios')
const Logger = require('../Logger')
const Database = require('../Database')

class GPodderSyncManager {
  constructor() {
    this.syncIntervals = new Map() // userId -> intervalId
  }

  /**
   * Initialize sync for all users with GPodder enabled
   */
  async init() {
    const users = await Database.userModel.findAll()
    for (const user of users) {
      if (user.extraData?.gpodder?.enabled) {
        this.startUserSync(user)
      }
    }
  }

  /**
   * Start periodic sync for a user
   */
  startUserSync(user) {
    const config = user.extraData?.gpodder
    if (!config?.enabled || !config.serverUrl) return

    // Clear existing interval
    this.stopUserSync(user.id)

    const intervalMs = (config.syncInterval || 30) * 60 * 1000
    const intervalId = setInterval(() => {
      this.syncUser(user.id)
    }, intervalMs)

    this.syncIntervals.set(user.id, intervalId)

    // Run initial sync
    this.syncUser(user.id)
  }

  /**
   * Full sync for a user
   */
  async syncUser(userId) {
    const user = await Database.userModel.findByPk(userId)
    if (!user?.extraData?.gpodder?.enabled) {
      this.stopUserSync(userId)
      return
    }

    try {
      await this.syncSubscriptions(user)
      await this.syncEpisodeActions(user)
    } catch (error) {
      Logger.error(`[GPodderSyncManager] Sync failed for user ${userId}: ${error.message}`)
    }
  }

  /**
   * Sync subscriptions with GPodder server
   */
  async syncSubscriptions(user) {
    const config = user.extraData.gpodder
    const since = config.lastSubscriptionSync || 0

    // GET subscription changes
    const response = await this.apiRequest(config, 'GET',
      `/index.php/apps/gpoddersync/subscriptions?since=${since}`)

    const { add = [], remove = [], timestamp } = response.data

    // Process additions - create virtual podcasts for new feeds
    for (const feedUrl of add) {
      await this.addPodcastFromFeed(user, feedUrl)
    }

    // Process removals - mark podcasts for attention (don't auto-delete)
    for (const feedUrl of remove) {
      Logger.info(`[GPodderSyncManager] Feed removed in GPodder: ${feedUrl}`)
      // Could emit socket event to notify user
    }

    // Push our subscription changes to server
    const localChanges = await this.getLocalSubscriptionChanges(user, since)
    if (localChanges.add.length || localChanges.remove.length) {
      await this.apiRequest(config, 'POST',
        '/index.php/apps/gpoddersync/subscription_change/create',
        localChanges)
    }

    // Update last sync timestamp
    user.extraData.gpodder.lastSubscriptionSync = timestamp
    user.changed('extraData', true)
    await user.save()
  }

  /**
   * Sync episode actions (play progress) with GPodder server
   */
  async syncEpisodeActions(user) {
    const config = user.extraData.gpodder
    const since = config.lastEpisodeActionSync || 0

    // GET episode actions from server
    const response = await this.apiRequest(config, 'GET',
      `/index.php/apps/gpoddersync/episode_action?since=${since}`)

    const { actions = [], timestamp } = response.data

    // Process incoming actions
    for (const action of actions) {
      await this.processEpisodeAction(user, action)
    }

    // Push our episode actions to server
    const localActions = await this.getLocalEpisodeActions(user, since)
    if (localActions.length) {
      await this.apiRequest(config, 'POST',
        '/index.php/apps/gpoddersync/episode_action/create',
        localActions)
    }

    // Update last sync timestamp
    user.extraData.gpodder.lastEpisodeActionSync = timestamp
    user.changed('extraData', true)
    await user.save()
  }

  /**
   * Process a single episode action from GPodder
   */
  async processEpisodeAction(user, action) {
    const { podcast, episode, guid, action: actionType, position, total, timestamp } = action

    // Find or create the podcast
    let podcastRecord = await Database.podcastModel.findOne({
      where: { feedURL: podcast }
    })

    if (!podcastRecord) {
      // Create virtual podcast entry
      podcastRecord = await this.addPodcastFromFeed(user, podcast)
      if (!podcastRecord) return
    }

    // Find or create the episode
    let episodeRecord = await Database.podcastEpisodeModel.findOne({
      where: {
        podcastId: podcastRecord.id,
        [Database.sequelize.Op.or]: [
          { 'extraData.guid': guid },
          { enclosureURL: episode }
        ]
      }
    })

    if (!episodeRecord && actionType === 'PLAY') {
      // Create virtual episode
      episodeRecord = await Database.podcastEpisodeModel.create({
        podcastId: podcastRecord.id,
        title: guid || 'Unknown Episode',
        enclosureURL: episode,
        isVirtual: true,
        duration: total > 0 ? total : null,
        extraData: { guid }
      })
    }

    if (!episodeRecord) return

    // Update media progress based on action
    if (actionType === 'PLAY' && position !== undefined) {
      const actionDate = new Date(timestamp)

      // Check if this action is newer than our progress
      const existingProgress = await Database.mediaProgressModel.findOne({
        where: {
          userId: user.id,
          mediaItemId: episodeRecord.id
        }
      })

      if (!existingProgress || existingProgress.updatedAt < actionDate) {
        await user.createUpdateMediaProgressFromPayload({
          libraryItemId: podcastRecord.libraryItemId,
          episodeId: episodeRecord.id,
          currentTime: position,
          duration: total > 0 ? total : episodeRecord.duration,
          isFinished: total > 0 && position >= total - 10 // Within 10 seconds of end
        })
      }
    }
  }

  /**
   * Make authenticated API request to GPodder server
   */
  async apiRequest(config, method, path, data = null) {
    const url = `${config.serverUrl}${path}`
    const auth = {
      username: config.username,
      password: config.password // Should be decrypted
    }

    return axios({
      method,
      url,
      auth,
      data,
      timeout: 30000,
      headers: {
        'Content-Type': 'application/json',
        'User-Agent': 'audiobookshelf (+https://audiobookshelf.org)'
      }
    })
  }

  /**
   * Add a podcast from RSS feed URL
   */
  async addPodcastFromFeed(user, feedUrl) {
    // Implementation similar to existing OPML import
    // But creates a "virtual" podcast without downloading episodes
    // ...
  }

  /**
   * Get local subscription changes since timestamp
   */
  async getLocalSubscriptionChanges(user, since) {
    // Query podcasts added/removed since timestamp
    // Return in GPodder format: { add: [...], remove: [...] }
  }

  /**
   * Get local episode actions since timestamp
   */
  async getLocalEpisodeActions(user, since) {
    // Query mediaProgress changes since timestamp
    // Convert to GPodder episode action format
  }

  stopUserSync(userId) {
    const intervalId = this.syncIntervals.get(userId)
    if (intervalId) {
      clearInterval(intervalId)
      this.syncIntervals.delete(userId)
    }
  }

  shutdown() {
    for (const intervalId of this.syncIntervals.values()) {
      clearInterval(intervalId)
    }
    this.syncIntervals.clear()
  }
}

module.exports = new GPodderSyncManager()
```

### 2.3 GPodder API Endpoints

**`server/controllers/GPodderController.js`**

```javascript
class GPodderController {
  /**
   * GET /api/me/gpodder/config
   * Get user's GPodder configuration
   */
  async getConfig(req, res) {
    const config = req.user.extraData?.gpodder || {}
    // Don't return password
    res.json({
      enabled: config.enabled || false,
      serverUrl: config.serverUrl || '',
      username: config.username || '',
      deviceId: config.deviceId || 'audiobookshelf',
      syncInterval: config.syncInterval || 30,
      lastSubscriptionSync: config.lastSubscriptionSync,
      lastEpisodeActionSync: config.lastEpisodeActionSync
    })
  }

  /**
   * POST /api/me/gpodder/config
   * Update user's GPodder configuration
   */
  async updateConfig(req, res) {
    const { enabled, serverUrl, username, password, deviceId, syncInterval } = req.body

    req.user.extraData = req.user.extraData || {}
    req.user.extraData.gpodder = {
      ...(req.user.extraData.gpodder || {}),
      enabled: !!enabled,
      serverUrl: serverUrl || null,
      username: username || null,
      deviceId: deviceId || 'audiobookshelf',
      syncInterval: syncInterval || 30
    }

    // Only update password if provided
    if (password) {
      req.user.extraData.gpodder.password = password // Should encrypt
    }

    req.user.changed('extraData', true)
    await req.user.save()

    // Start/stop sync based on enabled state
    if (enabled) {
      GPodderSyncManager.startUserSync(req.user)
    } else {
      GPodderSyncManager.stopUserSync(req.user.id)
    }

    res.json({ success: true })
  }

  /**
   * POST /api/me/gpodder/sync
   * Trigger manual sync
   */
  async triggerSync(req, res) {
    if (!req.user.extraData?.gpodder?.enabled) {
      return res.status(400).send('GPodder sync not enabled')
    }

    GPodderSyncManager.syncUser(req.user.id)
    res.json({ success: true, message: 'Sync started' })
  }

  /**
   * POST /api/me/gpodder/test
   * Test GPodder connection
   */
  async testConnection(req, res) {
    const { serverUrl, username, password } = req.body

    try {
      const response = await axios.get(
        `${serverUrl}/index.php/apps/gpoddersync/subscriptions`,
        {
          auth: { username, password },
          timeout: 10000
        }
      )
      res.json({ success: true, message: 'Connection successful' })
    } catch (error) {
      res.status(400).json({
        success: false,
        message: error.response?.status === 401
          ? 'Authentication failed'
          : 'Connection failed'
      })
    }
  }
}

module.exports = new GPodderController()
```

### 2.4 Route Registration

```javascript
// In ApiRouter.js - User routes section
this.router.get('/me/gpodder/config', MeController.getGPodderConfig.bind(this))
this.router.post('/me/gpodder/config', MeController.updateGPodderConfig.bind(this))
this.router.post('/me/gpodder/sync', MeController.triggerGPodderSync.bind(this))
this.router.post('/me/gpodder/test', MeController.testGPodderConnection.bind(this))
```

---

## Part 3: Implementation Phases

### Phase 1: Remote Streaming Foundation (Week 1-2)

1. **Database Migration**
   - Add `isVirtual` column to `podcastEpisodes`
   - Make `audioFile` nullable
   - Add indexes for performance

2. **Model Updates**
   - Modify `PodcastEpisode.getAudioTrack()` for remote URLs
   - Update `Podcast.checkCanDirectPlay()`

3. **Streaming Endpoint**
   - Create `/api/items/:id/episode/:episodeId/stream`
   - Implement proxy with range request support
   - Add SSRF protection

4. **Client Updates**
   - Update `AudioTrack.js` to handle remote streams
   - Test with various podcast feeds

### Phase 2: Virtual Episode Management (Week 2-3)

1. **Episode Creation**
   - API to create virtual episodes from feed data
   - UI to browse available episodes without downloading

2. **Progress Tracking**
   - Ensure `MediaProgress` works with virtual episodes
   - Update duration from actual playback

3. **Episode Lifecycle**
   - Convert virtual → local when downloaded
   - Keep metadata when local file deleted

### Phase 3: GPodder Sync (Week 3-4)

1. **User Configuration**
   - Settings UI for GPodder server configuration
   - Connection testing

2. **Sync Manager**
   - Subscription sync (bidirectional)
   - Episode action sync (bidirectional)

3. **Conflict Resolution**
   - Handle concurrent changes
   - Timestamp-based merging

### Phase 4: Testing & Polish (Week 4-5)

1. **Integration Testing**
   - Test with various GPodder servers (Nextcloud, opodsync)
   - Test with various podcast feeds

2. **Error Handling**
   - Network failures
   - Invalid feeds
   - Authentication errors

3. **Performance**
   - Optimize sync queries
   - Caching for feed metadata

---

## Files to Create/Modify

### New Files
- `server/managers/GPodderSyncManager.js`
- `server/controllers/GPodderController.js`
- `server/migrations/v2.X.X-virtual-episodes.js`
- `client/components/settings/GPodderSettings.vue`

### Modified Files
- `server/models/PodcastEpisode.js` - Add virtual episode support
- `server/models/Podcast.js` - Update direct play logic
- `server/controllers/PodcastController.js` - Add stream endpoint
- `server/routers/ApiRouter.js` - Register new routes
- `server/managers/PlaybackSessionManager.js` - Handle virtual episodes
- `server/models/User.js` - Add GPodder config to extraData
- `client/players/AudioTrack.js` - Handle remote URLs
- `client/players/PlayerHandler.js` - Minor adjustments

---

## Security Considerations

1. **SSRF Protection** - All remote URLs must go through `ssrf-req-filter`
2. **URL Validation** - Only allow http/https protocols
3. **Credential Storage** - Encrypt GPodder passwords at rest
4. **Rate Limiting** - Prevent abuse of proxy endpoint
5. **Content-Type Validation** - Only proxy audio content types

---

## Configuration Options

```javascript
// Server Settings (global)
{
  // Maximum concurrent remote streams per user
  maxRemoteStreamsPerUser: 3,

  // Timeout for remote stream connections
  remoteStreamTimeout: 30000,

  // Enable/disable remote streaming feature
  enableRemoteStreaming: true
}

// User Settings (per-user in extraData)
{
  gpodder: {
    enabled: true,
    serverUrl: 'https://nextcloud.example.com',
    username: 'user',
    password: 'encrypted...',
    deviceId: 'audiobookshelf',
    syncInterval: 30,  // minutes
    lastSubscriptionSync: 1704067200,
    lastEpisodeActionSync: 1704067200
  }
}
```

---

## API Reference

### Remote Streaming

```
GET /api/items/:libraryItemId/episode/:episodeId/stream
- Proxies remote enclosureURL
- Supports range requests for seeking
- Returns audio stream
```

### GPodder Configuration

```
GET /api/me/gpodder/config
- Returns current GPodder configuration (no password)

POST /api/me/gpodder/config
- Updates GPodder configuration
- Body: { enabled, serverUrl, username, password, deviceId, syncInterval }

POST /api/me/gpodder/test
- Tests connection to GPodder server
- Body: { serverUrl, username, password }

POST /api/me/gpodder/sync
- Triggers manual sync
```

---

## Open Questions

1. **Library Association**: Should virtual podcasts be associated with a specific library, or be user-specific?

2. **Download Queue**: Should virtual episodes appear in the download queue for easy conversion to local?

3. **Offline Support**: How should the mobile app handle virtual episodes? (Currently requires download)

4. **Feed Refresh**: Should we periodically refresh feed metadata for virtual episodes?

5. **Multi-User**: If multiple users sync the same podcast via GPodder, should they share one podcast entry or have separate ones?
