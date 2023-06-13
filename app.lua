function findFunctionMeta(meta)
	local match = 1
	for i, fun in ipairs(functions) do
		match = 1;
		for k, v in pairs(meta) do
			if fun.meta[k] ~= v then
				match = 0
			end
		end
		if match == 1 then
			return functions[i]
		end
	end
	return nil;
end

function findDeviceMeta(meta)
	devices = lynx.getDevices()
	local match = 1
	for i, dev in ipairs(devices) do
		match = 1;
		for k, v in pairs(meta) do
			if dev.meta[k] ~= v then
				match = 0
			end
		end
		if match == 1 then
			return devices[i]
		end
	end
	return nil;
end


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
		return dev.id
end

function fetchAndPublishData(station,parameter)
	local http_request = require "http.request"
	local url = "https://opendata-download-metobs.smhi.se/api/version/1.0/parameter/" .. parameter .. "/station/" .. station .. "/period/latest-hour/data.json"
	local headers, stream = assert(http_request.new_from_uri(url):go())
	local body = assert(stream:get_body_as_string())
	if headers:get ":status" ~= "200" then
	    -- error(body)
	    print("Could not fetch parameter: " .. parameter .. " from: " .. station)
	    return nil
	end

	local payload = json:decode(body)

	local func = findFunctionMeta({
		smhi_station_key = payload.station.key,
		smhi_parameter = payload.parameter.key
	})

	if func == nil then
		device = create_device_if_needed(payload)
		local fn = {
			type = "SMHI weatherstation data",
			installation_id = app.installation_id,
			meta = {
				name = payload.station.name .. " - " .. payload.parameter.name  .. " (" .. payload.parameter.summary .. ")",
				device_id = tostring(device),
				smhi_unit = tostring(payload.parameter.unit),
				smhi_station_key = tostring(payload.station.key),
				smhi_station_name = tostring(payload.station.name),
				smhi_parameter = tostring(payload.parameter.key),
				smhi_paramater_summary  = tostring(payload.parameter.summary),
				smhi_paramater_name  = tostring(payload.parameter.name),
				latitude = tostring(payload.position[1].latitude),
				longitude = tostring(payload.position[1].longitude),
				height = tostring(payload.position[1].height),
				data_url = url,
				topic_read = "obj/smhi/".. payload.station.key .."/" .. payload.parameter.key,
    			}
		}
		lynx.createFunction(fn)
	end

	if payload.value ~= nil and next(payload.value) ~= nil then
		print(json:encode(payload.value))
		local mqtt_payload = json:encode({value = payload.value[1].value, timestamp = payload.value[1].date / 1000, msg = "quality="..payload.value[1].quality})
		mq:pub("obj/smhi/".. payload.station.key .."/" .. payload.parameter.key, mqtt_payload, false, 0)
	end
end

function sendData()
	fetchAndPublishData(cfg.station, 1)
	fetchAndPublishData(cfg.station, 21)
	fetchAndPublishData(cfg.station, 39)
	fetchAndPublishData(cfg.station, 11)
	fetchAndPublishData(cfg.station, 22)
	fetchAndPublishData(cfg.station, 26)
	fetchAndPublishData(cfg.station, 27)
	fetchAndPublishData(cfg.station, 19)
	fetchAndPublishData(cfg.station, 2)
	fetchAndPublishData(cfg.station, 20)
	fetchAndPublishData(cfg.station, 9)
	fetchAndPublishData(cfg.station, 24)
	fetchAndPublishData(cfg.station, 40)
	fetchAndPublishData(cfg.station, 25)
	fetchAndPublishData(cfg.station, 28)
	fetchAndPublishData(cfg.station, 30)
	fetchAndPublishData(cfg.station, 32)
	fetchAndPublishData(cfg.station, 34)
	fetchAndPublishData(cfg.station, 36)
	fetchAndPublishData(cfg.station, 37)
	fetchAndPublishData(cfg.station, 29)
	fetchAndPublishData(cfg.station, 31)
	fetchAndPublishData(cfg.station, 33)
	fetchAndPublishData(cfg.station, 35)
	fetchAndPublishData(cfg.station, 17)
	fetchAndPublishData(cfg.station, 18)
	fetchAndPublishData(cfg.station, 15)
	fetchAndPublishData(cfg.station, 38)
	fetchAndPublishData(cfg.station, 23)
	fetchAndPublishData(cfg.station, 14)
	fetchAndPublishData(cfg.station, 5)
	fetchAndPublishData(cfg.station, 7)
	fetchAndPublishData(cfg.station, 6)
	fetchAndPublishData(cfg.station, 13)
	fetchAndPublishData(cfg.station, 12)
	fetchAndPublishData(cfg.station, 8)
	fetchAndPublishData(cfg.station, 10)
	fetchAndPublishData(cfg.station, 16)
	fetchAndPublishData(cfg.station, 4)
	fetchAndPublishData(cfg.station, 3)
end

function onStart()
	sendData()
	local t = timer:interval(cfg.interval * 3600, sendData)
end

