-- Finds a function by matching meta fields
function findFunctionMeta(meta)
    local match = 1
    for i, fun in ipairs(functions) do
        match = 1
        for k, v in pairs(meta) do
            if fun.meta[k] ~= v then
                match = 0
            end
        end
        if match == 1 then
            return functions[i]
        end
    end
    return nil
end

-- Finds a device by matching meta fields
function findDeviceMeta(meta)
    local devices = lynx.getDevices()
    local match = 1
    for i, dev in ipairs(devices) do
        match = 1
        for k, v in pairs(meta) do
            if dev.meta[k] ~= v then
                match = 0
            end
        end
        if match == 1 then
            return devices[i]
        end
    end
    return nil
end

-- Creates a device if it does not exist
function create_device_if_needed(payload)
    local dev = findDeviceMeta({
        station_key = payload.station.key
    })
    if dev == nil then
        local _dev = {
            type = "SMHI weatherstation",
            installation_id = app.installation_id,
            meta = {
                name = "SMHI station: " .. payload.station.name,
                station_key = tostring(payload.station.key),
                station_name = payload.station.name,
                station_owner = payload.station.owner,
                latitude = tostring(payload.position[1].latitude),
                longitude = tostring(payload.position[1].longitude)
            }
        }
        lynx.createDevice(_dev)
        dev = findDeviceMeta({
            station_key = payload.station.key
        })
    end
    return dev and dev.id or nil
end

-- Fetches SMHI data and publishes to MQTT
function fetchAndPublishData(station, parameter)
    local http_request = require "http.request"
    local stream = nil

    local ok, err = xpcall(function()
    	local url = "https://opendata-download-metobs.smhi.se/api/version/1.0/parameter/" ..
            parameter .. "/station/" .. station .. "/period/latest-hour/data.json"
    	local req = http_request.new_from_uri(url)
	    local headers

	    headers, stream = req:go(30)
    
        if not headers or headers:get(":status") ~= "200" then
	        if stream then
        	    pcall(function()
            		stream:shutdown()
            	end)
    	    end


            print(os.date("[%Y-%m-%d %H:%M:%S] ") ..
                "Could not fetch parameter: " ..
                tostring(parameter) ..
                " from: " .. tostring(station) ..
                " (HTTP status: " .. tostring(headers and headers:get(":status")) .. ")")
            return
        end

    	local body = stream:get_body_as_string()
	    stream:shutdown()
        stream = nil

    	local payload = json:decode(body)

    	local func = findFunctionMeta({
            smhi_station_key = payload.station.key,
            smhi_parameter = payload.parameter.key
        })
    
        if func == nil then
            local device = create_device_if_needed(payload)
            local fn = {
                type = "SMHI weatherstation data",
                installation_id = app.installation_id,
                meta = {
                    name                   = payload.station.name ..
                        " - " .. payload.parameter.name .. " (" .. payload.parameter.summary .. ")",
                    device_id              = tostring(device),
                    smhi_unit              = tostring(payload.parameter.unit),
                    smhi_station_key       = tostring(payload.station.key),
                    smhi_station_name      = tostring(payload.station.name),
                    smhi_parameter         = tostring(payload.parameter.key),
                    smhi_paramater_summary = tostring(payload.parameter.summary),
                    smhi_paramater_name    = tostring(payload.parameter.name),
                    latitude               = tostring(payload.position[1].latitude),
                    longitude              = tostring(payload.position[1].longitude),
                    height                 = tostring(payload.position[1].height),
                    data_url               = url,
                    topic_read             = "obj/smhi/" .. payload.station.key .. "/" .. payload.parameter.key,
                }
            }
            lynx.createFunction(fn)
        end
    
        if payload.value ~= nil and next(payload.value) ~= nil then
            print(os.date("[%Y-%m-%d %H:%M:%S] ") .. json:encode(payload.value))
            local mqtt_payload = json:encode({
                value = payload.value[1].value,
                timestamp = payload.value[1].date / 1000,
                msg = "quality=" .. payload.value[1].quality
            })
            mq:pub("obj/smhi/" .. payload.station.key .. "/" .. payload.parameter.key, mqtt_payload, false, 0)
        end

    end, debug.traceback)

    if stream then
        pcall(function()
            stream:shutdown()
        end)
    end

    if not ok then
        error(err)
    end

end

-- Polls all SMHI parameters, logs, and rate limits requests
function sendData()
    local parameters = { 1, 21, 39, 11, 22, 26, 27, 19, 2, 20, 9, 24, 40, 25, 28, 30, 32, 34, 36, 37, 29, 31, 33, 35, 17, 18, 15, 38, 23, 14, 5, 7, 6, 13, 12, 8, 10, 16, 4, 3 }
    print(os.date("[%Y-%m-%d %H:%M:%S] ") .. "sendData() called: polling SMHI data.")
    for _, param in ipairs(parameters) do
        local status, err = pcall(function()
            fetchAndPublishData(cfg.station, param)
        end)
        if not status then
            print(os.date("[%Y-%m-%d %H:%M:%S] ") ..
                "Error fetching parameter " .. tostring(param) .. ": " .. tostring(err))
        end
        os.execute("sleep 1")
    end
end

-- Starts polling and sets up timer, with logging and timer handle preservation
function onStart()
    sendData()
    _G.smhi_poll_timer = timer:interval(cfg.interval * 60, sendData)
    print(os.date("[%Y-%m-%d %H:%M:%S] ") ..
        "SMHI polling timer started with interval " .. tostring(cfg.interval) .. " minutes.")
end

-- Top-level error handler for robust app operation
local function main()
    onStart()
end

local ok, err = pcall(main)
if not ok then
    print(os.date("[%Y-%m-%d %H:%M:%S] ") .. "Top-level error: " .. tostring(err))
    print(debug.traceback())
end
