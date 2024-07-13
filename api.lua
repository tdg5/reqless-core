-------------------------------------------------------------------------------
-- This is the analog of the 'main' function when invoking reqless directly, as
-- apposed to for use within another library
-------------------------------------------------------------------------------
local ReqlessAPI = {}

-- Where possible, use config.getAll to fetch all configs
ReqlessAPI['config.get'] = function(now, key)
  if key then
    return Reqless.config.get(key)
  end
  return ReqlessAPI['config.getAll'](now)
end

ReqlessAPI['config.getAll'] = function(now)
  return cjson.encode(Reqless.config.get(nil))
end

ReqlessAPI['config.set'] = function(now, key, value)
  return Reqless.config.set(key, value)
end

-- Unset a configuration option
ReqlessAPI['config.unset'] = function(now, key)
  return Reqless.config.unset(key)
end

ReqlessAPI['failureGroups.counts'] = function(now, start, limit)
  return cjson.encode(Reqless.failed(nil, start, limit))
end

ReqlessAPI['job.addDependency'] = function(now, jid, ...)
  return Reqless.job(jid):depends(now, "on", unpack(arg))
end

ReqlessAPI['job.addTag'] = function(now, jid, ...)
  return cjson.encode(Reqless.tag(now, 'add', jid, unpack(arg)))
end

ReqlessAPI['job.cancel'] = function(now, ...)
  return Reqless.cancel(now, unpack(arg))
end

ReqlessAPI['job.complete'] = function(now, jid, worker, queue, data)
  return Reqless.job(jid):complete(now, worker, queue, data)
end

ReqlessAPI['job.completeAndRequeue'] = function(now, jid, worker, queue, data, next_queue, ...)
  return Reqless.job(jid):complete(now, worker, queue, data, 'next', next_queue, unpack(arg))
end

ReqlessAPI['job.fail'] = function(now, jid, worker, group, message, data)
  return Reqless.job(jid):fail(now, worker, group, message, data)
end

-- Return json for the job identified by the provided jid. If the job is not
-- present, then `nil` is returned
ReqlessAPI['job.get'] = function(now, jid)
  local data = Reqless.job(jid):data()
  if data then
    return cjson.encode(data)
  end
end

-- Return json blob of data or nil for each jid provided
ReqlessAPI['job.getMulti'] = function(now, ...)
  local results = {}
  for _, jid in ipairs(arg) do
    table.insert(results, Reqless.job(jid):data())
  end
  return cjson.encode(results)
end

ReqlessAPI['job.heartbeat'] = function(now, jid, worker, data)
  return Reqless.job(jid):heartbeat(now, worker, data)
end

-- Add logging to a particular jid
ReqlessAPI['job.log'] = function(now, jid, message, data)
  assert(jid, "Log(): Argument 'jid' missing")
  assert(message, "Log(): Argument 'message' missing")
  if data then
    data = assert(cjson.decode(data),
      "Log(): Argument 'data' not cjson: " .. tostring(data))
  end

  local job = Reqless.job(jid)
  assert(job:exists(), 'Log(): Job ' .. jid .. ' does not exist')
  job:history(now, message, data)
end

ReqlessAPI['job.requeue'] = function(now, worker, queue, jid, ...)
  local job = Reqless.job(jid)
  assert(job:exists(), 'Requeue(): Job ' .. jid .. ' does not exist')
  return ReqlessAPI['queue.put'](now, worker, queue, jid, unpack(arg))
end

ReqlessAPI['job.removeDependency'] = function(now, jid, ...)
  return Reqless.job(jid):depends(now, "off", unpack(arg))
end

ReqlessAPI['job.removeTag'] = function(now, jid, ...)
  return cjson.encode(Reqless.tag(now, 'remove', jid, unpack(arg)))
end

ReqlessAPI['job.retry'] = function(now, jid, queue, worker, delay, group, message)
  return Reqless.job(jid):retry(now, queue, worker, delay, group, message)
end

ReqlessAPI['job.setPriority'] = function(now, jid, priority)
  return Reqless.job(jid):priority(priority)
end

ReqlessAPI['job.timeout'] = function(now, ...)
  for _, jid in ipairs(arg) do
    Reqless.job(jid):timeout(now)
  end
end

ReqlessAPI['job.track'] = function(now, jid)
  return cjson.encode(Reqless.track(now, 'track', jid))
end

ReqlessAPI['job.untrack'] = function(now, jid)
  return cjson.encode(Reqless.track(now, 'untrack', jid))
end

ReqlessAPI["jobs.completed"] = function(now, offset, limit)
  return Reqless.jobs(now, 'complete', offset, limit)
end

ReqlessAPI['jobs.failedByGroup'] = function(now, group, start, limit)
  return cjson.encode(Reqless.failed(group, start, limit))
end

ReqlessAPI['jobs.tagged'] = function(now, tag, ...)
  return cjson.encode(Reqless.tag(now, 'get', tag, unpack(arg)))
end

ReqlessAPI['jobs.tracked'] = function(now)
  return cjson.encode(Reqless.track(now))
end

ReqlessAPI['queue.counts'] = function(now, queue)
  return cjson.encode(ReqlessQueue.counts(now, queue))
end

ReqlessAPI['queue.forget'] = function(now, ...)
  ReqlessQueue.deregister(unpack(arg))
end

ReqlessAPI["queue.jobsByState"] = function(now, state, ...)
  return Reqless.jobs(now, state, unpack(arg))
end

ReqlessAPI['queue.length'] = function(now, queue)
  return Reqless.queue(queue):length()
end

ReqlessAPI['queue.pause'] = function(now, ...)
  return ReqlessQueue.pause(now, unpack(arg))
end

ReqlessAPI['queue.peek'] = function(now, queue, offset, count)
  local jids = Reqless.queue(queue):peek(now, offset, count)
  local response = {}
  for _, jid in ipairs(jids) do
    table.insert(response, Reqless.job(jid):data())
  end
  return cjson.encode(response)
end

ReqlessAPI['queue.pop'] = function(now, queue, worker, count)
  local jids = Reqless.queue(queue):pop(now, worker, count)
  local response = {}
  for _, jid in ipairs(jids) do
    table.insert(response, Reqless.job(jid):data())
  end
  return cjson.encode(response)
end

ReqlessAPI['queue.put'] = function(now, worker, queue, jid, klass, data, delay, ...)
  return Reqless.queue(queue):put(now, worker, jid, klass, data, delay, unpack(arg))
end

ReqlessAPI['queue.recur'] = function(now, queue, jid, klass, data, spec, ...)
  return Reqless.queue(queue):recur(now, jid, klass, data, spec, unpack(arg))
end

ReqlessAPI['queue.stats'] = function(now, queue, date)
  return cjson.encode(Reqless.queue(queue):stats(now, date))
end

ReqlessAPI['queue.throttle.get'] = function(now, queue)
  local data = Reqless.throttle(ReqlessQueue.ns .. queue):data()
  if data then
    return cjson.encode(data)
  end
end

ReqlessAPI['queue.throttle.set'] = function(now, queue, max)
  Reqless.throttle(ReqlessQueue.ns .. queue):set({maximum = max}, 0)
end

ReqlessAPI['queue.unfail'] = function(now, queue, group, count)
  return Reqless.queue(queue):unfail(now, group, count)
end

ReqlessAPI['queue.unpause'] = function(now, ...)
  return ReqlessQueue.unpause(unpack(arg))
end

ReqlessAPI['queues.list'] = function(now)
  return cjson.encode(ReqlessQueue.counts(now, nil))
end

ReqlessAPI['recurringJob.get'] = function(now, jid)
  local data = Reqless.recurring(jid):data()
  if data then
    return cjson.encode(data)
  end
end

ReqlessAPI['recurringJob.addTag'] = function(now, jid, ...)
  return Reqless.recurring(jid):tag(unpack(arg))
end

ReqlessAPI['recurringJob.removeTag'] = function(now, jid, ...)
  return Reqless.recurring(jid):untag(unpack(arg))
end

ReqlessAPI['recurringJob.unrecur'] = function(now, jid)
  return Reqless.recurring(jid):unrecur()
end

ReqlessAPI['recurringJob.update'] = function(now, jid, ...)
  return Reqless.recurring(jid):update(now, unpack(arg))
end

ReqlessAPI['tags.top'] = function(now, ...)
  return cjson.encode(Reqless.tag(now, 'top', unpack(arg)))
end

ReqlessAPI['throttle.delete'] = function(now, tid)
  return Reqless.throttle(tid):unset()
end

ReqlessAPI['throttle.get'] = function(now, tid)
  return cjson.encode(Reqless.throttle(tid):data())
end

ReqlessAPI['throttle.locks'] = function(now, tid)
  return Reqless.throttle(tid).locks.members()
end

ReqlessAPI['throttle.pending'] = function(now, tid)
  return Reqless.throttle(tid).pending.members()
end

-- releases the set of jids from the specified throttle.
ReqlessAPI['throttle.release'] = function(now, tid, ...)
  local throttle = Reqless.throttle(tid)

  for _, jid in ipairs(arg) do
    throttle:release(now, jid)
  end
end

ReqlessAPI['throttle.set'] = function(now, tid, max, ...)
  local expiration = unpack(arg)
  local data = {
    maximum = max
  }
  Reqless.throttle(tid):set(data, tonumber(expiration or 0))
end

ReqlessAPI['throttle.ttl'] = function(now, tid)
  return Reqless.throttle(tid):ttl()
end

ReqlessAPI['worker.counts'] = function(now, worker)
  return cjson.encode(ReqlessWorker.counts(now, worker))
end

ReqlessAPI['worker.unregister'] = function(now, ...)
  return ReqlessWorker.deregister(unpack(arg))
end

ReqlessAPI['workers.list'] = function(now)
  return cjson.encode(ReqlessWorker.counts(now, nil))
end

-------------------------------------------------------------------------------
-- Deprecated APIs
-------------------------------------------------------------------------------

-- Deprecated. Use job.cancel instead.
ReqlessAPI['cancel'] = function(now, ...)
  return ReqlessAPI['job.cancel'](now, unpack(arg))
end

-- Deprecated. Use job.complete instead.
ReqlessAPI['complete'] = function(now, jid, worker, queue, data, ...)
  -- Call job:complete directly because optional args could be in any order
  return Reqless.job(jid):complete(now, worker, queue, data, unpack(arg))
end

-- Deprecated. Use job.addDependency or job.removeDependency instead.
ReqlessAPI['depends'] = function(now, jid, command, ...)
  if command == "on" then
    return ReqlessAPI['job.addDependency'](now, jid, unpack(arg))
  elseif command == "off" then
    return ReqlessAPI['job.removeDependency'](now, jid, unpack(arg))
  end
  error('Depends(): Argument "command" must be "on" or "off"')
end

-- Deprecated. Use job.fail instead.
ReqlessAPI['fail'] = function(now, jid, worker, group, message, data)
  return ReqlessAPI['job.fail'](now, jid, worker, group, message, data)
end

-- Deprecated. Use failureGroups.counts or jobs.failedByGroup instead.
ReqlessAPI['failed'] = function(now, group, start, limit)
  if group then
    return ReqlessAPI['jobs.failedByGroup'](now, group, start, limit)
  end
  return ReqlessAPI['failureGroups.counts'](now, start, limit)
end

-- Deprecated. Use job.get instead.
ReqlessAPI['get'] = function(now, jid)
  return ReqlessAPI['job.get'](now, jid)
end

-- Deprecated. Use job.heartbeat instead.
ReqlessAPI.heartbeat = function(now, jid, worker, data)
  return ReqlessAPI['job.heartbeat'](now, jid, worker, data)
end

-- Deprecated. Use jobs.completed or queue.jobsByState instead.
ReqlessAPI['jobs'] = function(now, state, ...)
  if state == 'complete' then
    return ReqlessAPI['jobs.completed'](now, unpack(arg))
  end
  return ReqlessAPI['queue.jobsByState'](now, state, unpack(arg))
end

-- Deprecated. Use queue.length instead.
ReqlessAPI['length'] = function(now, queue)
  return ReqlessAPI['queue.length'](now, queue)
end

-- Deprecated. Use job.log instead.
ReqlessAPI['log'] = function(now, jid, message, data)
  return ReqlessAPI['job.log'](now, jid, message, data)
end

-- Deprecated. Use job.getMulti instead.
ReqlessAPI['multiget'] = function(now, ...)
  return ReqlessAPI['job.getMulti'](now, unpack(arg))
end

-- Deprecated. Use queue.pause instead.
ReqlessAPI['pause'] = function(now, ...)
  return ReqlessAPI['queue.pause'](now, unpack(arg))
end

-- Deprecated. Use queue.peek instead.
ReqlessAPI['peek'] = function(now, queue, offset, count)
  return ReqlessAPI['queue.peek'](now, queue, offset, count)
end

-- Deprecated. Use queue.pop instead.
ReqlessAPI['pop'] = function(now, queue, worker, count)
  return ReqlessAPI['queue.pop'](now, queue, worker, count)
end

-- Deprecated. Use job.setPriority instead.
ReqlessAPI['priority'] = function(now, jid, priority)
  return ReqlessAPI['job.setPriority'](now, jid, priority)
end

-- Deprecated. Use queue.pop instead.
ReqlessAPI['put'] = function(now, worker, queue, jid, klass, data, delay, ...)
  return ReqlessAPI['queue.put'](now, worker, queue, jid, klass, data, delay, unpack(arg))
end

-- Deprecated. Use queue.counts or queues.list instead.
ReqlessAPI['queues'] = function(now, queue)
  if queue then
    return ReqlessAPI['queue.counts'](now, queue)
  end
  return ReqlessAPI['queues.list'](now)
end

-- Deprecated. Use queue.recur instead.
ReqlessAPI['recur'] = function(now, queue, jid, klass, data, spec, ...)
  return ReqlessAPI['queue.recur'](now, queue, jid, klass, data, spec, unpack(arg))
end

-- Deprecated. Use recurringJob.get instead.
ReqlessAPI['recur.get'] = function(now, jid)
  return ReqlessAPI['recurringJob.get'](now, jid)
end

-- Deprecated. Use recurringJob.addTag instead.
ReqlessAPI['recur.tag'] = function(now, jid, ...)
  return ReqlessAPI['recurringJob.addTag'](now, jid, unpack(arg))
end

-- Deprecated. Use recurringJob.removeTag instead.
ReqlessAPI['recur.untag'] = function(now, jid, ...)
  return ReqlessAPI['recurringJob.removeTag'](now, jid, unpack(arg))
end

-- Deprecated. Use recurringJob.update instead.
ReqlessAPI['recur.update'] = function(now, jid, ...)
  return ReqlessAPI['recurringJob.update'](now, jid, unpack(arg))
end

-- Deprecated. Use job.requeue instead.
ReqlessAPI['requeue'] = function(now, worker, queue, jid, ...)
  return ReqlessAPI['job.requeue'](now, worker, queue, jid, unpack(arg))
end

-- Deprecated. Use job.retry instead.
ReqlessAPI['retry'] = function(now, jid, queue, worker, delay, group, message)
  return ReqlessAPI['job.retry'](now, jid, queue, worker, delay, group, message)
end

-- Deprecated. Use queue.stats instead.
ReqlessAPI['stats'] = function(now, queue, date)
  return ReqlessAPI['queue.stats'](now, queue, date)
end

-- Deprecated. Use job.addTag, job.removeTag, jobs.tagged, or tags.top instead.
ReqlessAPI['tag'] = function(now, command, ...)
  if command == 'add' then
    return ReqlessAPI['job.addTag'](now, unpack(arg))
  end
  if command == 'remove' then
    return ReqlessAPI['job.removeTag'](now, unpack(arg))
  end
  if command == 'get' then
    return ReqlessAPI['jobs.tagged'](now, unpack(arg))
  end
  if command == 'top' then
    return ReqlessAPI['tags.top'](now, unpack(arg))
  end
  error('Tag(): Unknown command ' .. command)
end

-- Deprecated. Use job.timeout instead.
ReqlessAPI['timeout'] = function(now, ...)
  return ReqlessAPI['job.timeout'](now, unpack(arg))
end

-- Deprecated. Use job.track, job.untrack, or jobs.tracked instead.
ReqlessAPI['track'] = function(now, command, jid)
  if command == 'track' then
    return ReqlessAPI['job.track'](now, jid)
  end

  if command == 'untrack' then
    return ReqlessAPI['job.untrack'](now, jid)
  end

  return ReqlessAPI['jobs.tracked'](now)
end

-- Deprecated. Use queue.unfail instead.
ReqlessAPI['unfail'] = function(now, queue, group, count)
  return ReqlessAPI['queue.unfail'](now, queue, group, count)
end

-- Deprecated. Use queue.unpause instead.
ReqlessAPI['unpause'] = function(now, ...)
  return ReqlessAPI['queue.unpause'](now, unpack(arg))
end

-- Deprecated. Use recurringJob.unrecur instead.
ReqlessAPI['unrecur'] = function(now, jid)
  return ReqlessAPI['recurringJob.unrecur'](now, jid)
end

-- Deprecated. Use worker.unregister instead.
ReqlessAPI['worker.deregister'] = function(now, ...)
  return ReqlessAPI['worker.unregister'](now, unpack(arg))
end

-- Deprecated. Use worker.counts or workers.list instead
ReqlessAPI['workers'] = function(now, worker)
  if worker then
    return ReqlessAPI['worker.counts'](now, worker)
  end
  return ReqlessAPI['workers.list'](now)
end

-------------------------------------------------------------------------------
-- Function lookup
-------------------------------------------------------------------------------

-- None of the function calls accept keys
if #KEYS > 0 then error('No Keys should be provided') end

-- The first argument must be the function that we intend to call, and it must
-- exist
local command_name = assert(table.remove(ARGV, 1), 'Must provide a command')
local command      = assert(
  ReqlessAPI[command_name], 'Unknown command ' .. command_name)

-- The second argument should be the current time from the requesting client
local now          = tonumber(table.remove(ARGV, 1))
local now          = assert(
  now, 'Arg "now" missing or not a number: ' .. (now or 'nil'))

return command(now, unpack(ARGV))
