-------------------------------------------------------------------------------
-- This is the analog of the 'main' function when invoking qless directly, as
-- apposed to for use within another library
-------------------------------------------------------------------------------
local QlessAPI = {}

QlessAPI['config.get'] = function(now, key)
  if key then
    return Qless.config.get(key)
  end
  return cjson.encode(Qless.config.get(nil))
end

QlessAPI['config.set'] = function(now, key, value)
  return Qless.config.set(key, value)
end

-- Unset a configuration option
QlessAPI['config.unset'] = function(now, key)
  return Qless.config.unset(key)
end

QlessAPI['job.cancel'] = function(now, ...)
  return Qless.cancel(now, unpack(arg))
end

QlessAPI['job.complete'] = function(now, jid, worker, queue, data, ...)
  return Qless.job(jid):complete(now, worker, queue, data, unpack(arg))
end

QlessAPI['job.depends'] = function(now, jid, command, ...)
  return Qless.job(jid):depends(now, command, unpack(arg))
end

QlessAPI['job.fail'] = function(now, jid, worker, group, message, data)
  return Qless.job(jid):fail(now, worker, group, message, data)
end

-- Return json for the job identified by the provided jid. If the job is not
-- present, then `nil` is returned
QlessAPI['job.get'] = function(now, jid)
  local data = Qless.job(jid):data()
  if data then
    return cjson.encode(data)
  end
end

-- Return json blob of data or nil for each jid provided
QlessAPI['job.getMulti'] = function(now, ...)
  local results = {}
  for _, jid in ipairs(arg) do
    table.insert(results, Qless.job(jid):data())
  end
  return cjson.encode(results)
end

QlessAPI['job.heartbeat'] = function(now, jid, worker, data)
  return Qless.job(jid):heartbeat(now, worker, data)
end

-- Add logging to a particular jid
QlessAPI['job.log'] = function(now, jid, message, data)
  assert(jid, "Log(): Argument 'jid' missing")
  assert(message, "Log(): Argument 'message' missing")
  if data then
    data = assert(cjson.decode(data),
      "Log(): Argument 'data' not cjson: " .. tostring(data))
  end

  local job = Qless.job(jid)
  assert(job:exists(), 'Log(): Job ' .. jid .. ' does not exist')
  job:history(now, message, data)
end

QlessAPI['job.priority'] = function(now, jid, priority)
  return Qless.job(jid):priority(priority)
end

QlessAPI['job.retry'] = function(now, jid, queue, worker, delay, group, message)
  return Qless.job(jid):retry(now, queue, worker, delay, group, message)
end

QlessAPI['job.timeout'] = function(now, ...)
  for _, jid in ipairs(arg) do
    Qless.job(jid):timeout(now)
  end
end

QlessAPI['job.track'] = function(now, jid)
  return cjson.encode(Qless.track(now, 'track', jid))
end

QlessAPI['job.untrack'] = function(now, jid)
  return cjson.encode(Qless.track(now, 'untrack', jid))
end

QlessAPI["jobs.completed"] = function(now, offset, limit)
  return Qless.jobs(now, 'complete', offset, limit)
end

QlessAPI['jobs.failed'] = function(now, group, start, limit)
  return cjson.encode(Qless.failed(group, start, limit))
end

QlessAPI['jobs.tracked'] = function(now)
  return cjson.encode(Qless.track(now))
end

QlessAPI['queue.counts'] = function(now, queue)
  return cjson.encode(QlessQueue.counts(now, queue))
end

QlessAPI["queue.jobsByState"] = function(now, state, ...)
  return Qless.jobs(now, state, unpack(arg))
end

QlessAPI['queue.peek'] = function(now, queue, offset, count)
  local jids = Qless.queue(queue):peek(now, offset, count)
  local response = {}
  for _, jid in ipairs(jids) do
    table.insert(response, Qless.job(jid):data())
  end
  return cjson.encode(response)
end

QlessAPI['queue.pop'] = function(now, queue, worker, count)
  local jids = Qless.queue(queue):pop(now, worker, count)
  local response = {}
  for _, jid in ipairs(jids) do
    table.insert(response, Qless.job(jid):data())
  end
  return cjson.encode(response)
end

QlessAPI['queue.put'] = function(now, worker, queue, jid, klass, data, delay, ...)
  return Qless.queue(queue):put(now, worker, jid, klass, data, delay, unpack(arg))
end

QlessAPI['queue.stats'] = function(now, queue, date)
  return cjson.encode(Qless.queue(queue):stats(now, date))
end

QlessAPI['queues.list'] = function(now)
  return cjson.encode(QlessQueue.counts(now, nil))
end

QlessAPI['worker.counts'] = function(now, worker)
  return cjson.encode(QlessWorker.counts(now, worker))
end

QlessAPI['workers.list'] = function(now)
  return cjson.encode(QlessWorker.counts(now, nil))
end

QlessAPI['tag'] = function(now, command, ...)
  return cjson.encode(Qless.tag(now, command, unpack(arg)))
end

QlessAPI['pause'] = function(now, ...)
  return QlessQueue.pause(now, unpack(arg))
end

QlessAPI['unpause'] = function(now, ...)
  return QlessQueue.unpause(unpack(arg))
end

QlessAPI['requeue'] = function(now, worker, queue, jid, ...)
  local job = Qless.job(jid)
  assert(job:exists(), 'Requeue(): Job ' .. jid .. ' does not exist')
  return QlessAPI['queue.put'](now, worker, queue, jid, unpack(arg))
end

QlessAPI['unfail'] = function(now, queue, group, count)
  return Qless.queue(queue):unfail(now, group, count)
end

-- Recurring job stuff
QlessAPI['recur'] = function(now, queue, jid, klass, data, spec, ...)
  return Qless.queue(queue):recur(now, jid, klass, data, spec, unpack(arg))
end

QlessAPI['unrecur'] = function(now, jid)
  return Qless.recurring(jid):unrecur()
end

QlessAPI['recur.get'] = function(now, jid)
  local data = Qless.recurring(jid):data()
  if not data then
    return nil
  end
  return cjson.encode(data)
end

QlessAPI['recur.update'] = function(now, jid, ...)
  return Qless.recurring(jid):update(now, unpack(arg))
end

QlessAPI['recur.tag'] = function(now, jid, ...)
  return Qless.recurring(jid):tag(unpack(arg))
end

QlessAPI['recur.untag'] = function(now, jid, ...)
  return Qless.recurring(jid):untag(unpack(arg))
end

QlessAPI['length'] = function(now, queue)
  return Qless.queue(queue):length()
end

QlessAPI['worker.deregister'] = function(now, ...)
  return QlessWorker.deregister(unpack(arg))
end

QlessAPI['queue.forget'] = function(now, ...)
  QlessQueue.deregister(unpack(arg))
end

QlessAPI['queue.throttle.get'] = function(now, queue)
  local data = Qless.throttle(QlessQueue.ns .. queue):data()
  if not data then
    return nil
  end
  return cjson.encode(data)
end

QlessAPI['queue.throttle.set'] = function(now, queue, max)
  Qless.throttle(QlessQueue.ns .. queue):set({maximum = max}, 0)
end

-- Throttle apis
QlessAPI['throttle.set'] = function(now, tid, max, ...)
  local expiration = unpack(arg)
  local data = {
    maximum = max
  }
  Qless.throttle(tid):set(data, tonumber(expiration or 0))
end

QlessAPI['throttle.get'] = function(now, tid)
  return cjson.encode(Qless.throttle(tid):data())
end

QlessAPI['throttle.delete'] = function(now, tid)
  return Qless.throttle(tid):unset()
end

QlessAPI['throttle.locks'] = function(now, tid)
  return Qless.throttle(tid).locks.members()
end

QlessAPI['throttle.pending'] = function(now, tid)
  return Qless.throttle(tid).pending.members()
end

QlessAPI['throttle.ttl'] = function(now, tid)
  return Qless.throttle(tid):ttl()
end

-- releases the set of jids from the specified throttle.
QlessAPI['throttle.release'] = function(now, tid, ...)
  local throttle = Qless.throttle(tid)

  for _, jid in ipairs(arg) do
    throttle:release(now, jid)
  end
end

-------------------------------------------------------------------------------
-- Deprecated APIs
-------------------------------------------------------------------------------

-- Deprecated. Use job.cancel instead.
QlessAPI['cancel'] = function(now, ...)
  return QlessAPI['job.cancel'](now, unpack(arg))
end

-- Deprecated. Use job.complete instead.
QlessAPI['complete'] = function(now, jid, worker, queue, data, ...)
  return QlessAPI['job.complete'](now, jid, worker, queue, data, unpack(arg))
end

-- Deprecated. Use job.depends instead.
QlessAPI['depends'] = function(now, jid, command, ...)
  return QlessAPI['job.depends'](now, jid, command, unpack(arg))
end

-- Deprecated. Use job.fail instead.
QlessAPI['fail'] = function(now, jid, worker, group, message, data)
  return QlessAPI['job.fail'](now, jid, worker, group, message, data)
end

-- Deprecated. Use jobs.failed instead.
QlessAPI['failed'] = function(now, group, start, limit)
  return QlessAPI['jobs.failed'](now, group, start, limit)
end

-- Deprecated. Use job.get instead.
QlessAPI['get'] = function(now, jid)
  return QlessAPI['job.get'](now, jid)
end

-- Deprecated. Use job.heartbeat instead.
QlessAPI.heartbeat = function(now, jid, worker, data)
  return QlessAPI['job.heartbeat'](now, jid, worker, data)
end

-- Deprecated. Use jobs.completed or queue.jobs.byState instead.
QlessAPI['jobs'] = function(now, state, ...)
  if state == 'complete' then
    return QlessAPI['jobs.completed'](now, unpack(arg))
  end
  return QlessAPI['queue.jobsByState'](now, state, unpack(arg))
end

-- Deprecated. Use job.log instead.
QlessAPI['log'] = function(now, jid, message, data)
  return QlessAPI['job.log'](now, jid, message, data)
end

-- Deprecated. Use job.getMulti instead.
QlessAPI['multiget'] = function(now, ...)
  return QlessAPI['job.getMulti'](now, unpack(arg))
end

-- Deprecated. Use queue.peek instead.
QlessAPI['peek'] = function(now, queue, offset, count)
  return QlessAPI['queue.peek'](now, queue, offset, count)
end

-- Deprecated. Use queue.pop instead.
QlessAPI['pop'] = function(now, queue, worker, count)
  return QlessAPI['queue.pop'](now, queue, worker, count)
end

-- Deprecated. Use job.priority instead.
QlessAPI['priority'] = function(now, jid, priority)
  return QlessAPI['job.priority'](now, jid, priority)
end

-- Deprecated. Use queue.pop instead.
QlessAPI['put'] = function(now, worker, queue, jid, klass, data, delay, ...)
  return QlessAPI['queue.put'](now, worker, queue, jid, klass, data, delay, unpack(arg))
end

-- Deprecated. Use queue.counts or queues.list instead.
QlessAPI['queues'] = function(now, queue)
  if queue then
    return QlessAPI['queue.counts'](now, queue)
  end
  return QlessAPI['queues.list'](now)
end

-- Deprecated. Use job.retry instead.
QlessAPI['retry'] = function(now, jid, queue, worker, delay, group, message)
  return QlessAPI['job.retry'](now, jid, queue, worker, delay, group, message)
end

-- Deprecated. Use queue.stats instead.
QlessAPI['stats'] = function(now, queue, date)
  return QlessAPI['queue.stats'](now, queue, date)
end

-- Deprecated. Use job.timeout instead.
QlessAPI['timeout'] = function(now, ...)
  return QlessAPI['job.timeout'](now, unpack(arg))
end

-- Deprecated. Use job.track, job.untrack, or jobs.tracked instead.
QlessAPI['track'] = function(now, command, jid)
  if command == 'track' then
    return QlessAPI['job.track'](now, jid)
  end

  if command == 'untrack' then
    return QlessAPI['job.untrack'](now, jid)
  end

  return QlessAPI['jobs.tracked'](now)
end

-- Deprecated. Use worker.counts or workers.list instead
QlessAPI['workers'] = function(now, worker)
  if worker then
    return QlessAPI['worker.counts'](now, worker)
  end
  return QlessAPI['workers.list'](now)
end

-------------------------------------------------------------------------------
-- Function lookup
-------------------------------------------------------------------------------

-- None of the qless function calls accept keys
if #KEYS > 0 then error('No Keys should be provided') end

-- The first argument must be the function that we intend to call, and it must
-- exist
local command_name = assert(table.remove(ARGV, 1), 'Must provide a command')
local command      = assert(
  QlessAPI[command_name], 'Unknown command ' .. command_name)

-- The second argument should be the current time from the requesting client
local now          = tonumber(table.remove(ARGV, 1))
local now          = assert(
  now, 'Arg "now" missing or not a number: ' .. (now or 'nil'))

return command(now, unpack(ARGV))
