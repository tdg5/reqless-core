all: reqless.lua reqless-lib.lua

reqless-lib.lua: util.lua base.lua config.lua job.lua queue.lua recurring.lua worker.lua throttle.lua
	echo "-- Current SHA: `git rev-parse HEAD`" > reqless-lib.lua
	echo "-- This is a generated file" >> reqless-lib.lua
	cat util.lua base.lua config.lua job.lua queue.lua recurring.lua worker.lua throttle.lua >> reqless-lib.lua

reqless.lua: reqless-lib.lua api.lua
	# Cat these files out, but remove all the comments from the source
	echo "-- Current SHA: `git rev-parse HEAD`" > reqless.lua
	echo "-- This is a generated file" >> reqless.lua
	cat reqless-lib.lua api.lua | \
		egrep -v '^[[:space:]]*--[^\[]' | \
		egrep -v '^--$$' >> reqless.lua

clean:
	rm -f reqless.lua reqless-lib.lua

.PHONY: test
test: reqless.lua *.lua
	pytest

.PHONY: test-watch
test-watch:
	ptw . --patterns '*.lua,*.py' --runner scripts/pytest
