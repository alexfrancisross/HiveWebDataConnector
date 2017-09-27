(function() {
    // Create the connector object
    var myConnector = tableau.makeConnector();

    // Define the schema
    myConnector.getSchema = function(schemaCallback) {

        var heatingSchedule_cols = [{
                id: "tableId",
                alias: "table id",
                dataType: tableau.dataTypeEnum.string
            }, {
                id: "deviceId",
                alias: "device id",
                dataType: tableau.dataTypeEnum.string
            }, {
                id: "time",
                alias: "time",
                dataType: tableau.dataTypeEnum.string
            },
            {
                id: "heatCoolMode",
                alias: "heatCoolMode",
                dataType: tableau.dataTypeEnum.string
            }, {
                id: "targetHeatTemperature",
                alias: "targetHeatTemperature",
                dataType: tableau.dataTypeEnum.float
            }, {
                id: "weekday",
                alias: "weekday",
                dataType: tableau.dataTypeEnum.string
            }
        ]
        var heatingScheduleTableSchema = {
            id: "heating",
            alias: "Heating Schedule",
            columns: heatingSchedule_cols
        };

        var hotwaterSchedule_cols = [{
                id: "tableId",
                alias: "table id",
                dataType: tableau.dataTypeEnum.string
            }, {
                id: "deviceId",
                alias: "device id",
                dataType: tableau.dataTypeEnum.string
            }, {
                id: "time",
                alias: "time",
                dataType: tableau.dataTypeEnum.string
            },
            {
                id: "heatCoolMode",
                alias: "heatCoolMode",
                dataType: tableau.dataTypeEnum.string
            }, {
                id: "targetHeatTemperature",
                alias: "targetHeatTemperature",
                dataType: tableau.dataTypeEnum.float
            }, {
                id: "weekday",
                alias: "weekday",
                dataType: tableau.dataTypeEnum.string
            }
        ]
        var hotwaterScheduleTableSchema = {
            id: "hotwater",
            alias: "Hot Water Schedule",
            columns: hotwaterSchedule_cols
        };

        var temperature_cols = [{
                id: "tableId",
                alias: "table id",
                dataType: tableau.dataTypeEnum.string
            }, {
                id: "deviceId",
                alias: "device id",
                dataType: tableau.dataTypeEnum.string
            },
            {
                id: "startTime",
                alias: "startTime",
                dataType: tableau.dataTypeEnum.int
            }, {
                id: "formattedStartTime",
                alias: "formattedStartTime",
                dataType: tableau.dataTypeEnum.datetime
            }, {
                id: "temperature",
                alias: "temperature",
                dataType: tableau.dataTypeEnum.float
            }
        ];

        var temperatureTableSchema = {
            id: "temperature",
            alias: "Temperature",
            columns: temperature_cols
        };

        var target_temperature_cols = [{
                id: "tableId",
                alias: "table id",
                dataType: tableau.dataTypeEnum.string
            }, {
                id: "deviceId",
                alias: "device id",
                dataType: tableau.dataTypeEnum.string
            },
            {
                id: "startTime",
                alias: "startTime",
                dataType: tableau.dataTypeEnum.int
            }, {
                id: "formattedStartTime",
                alias: "formattedStartTime",
                dataType: tableau.dataTypeEnum.datetime
            }, {
                id: "temperature",
                alias: "temperature",
                dataType: tableau.dataTypeEnum.float
            }
        ];

        var targetTemperatureTableSchema = {
            id: "targetTemperature",
            alias: "Target Temperature",
            columns: target_temperature_cols
        };

        schemaCallback([temperatureTableSchema, targetTemperatureTableSchema, heatingScheduleTableSchema, hotwaterScheduleTableSchema]);
    };

    // Download the data
    myConnector.getData = function(table, doneCallback) {
        var connectionData = JSON.parse(tableau.connectionData);

        var url = 'https://api-prod.bgchprod.info:443/omnia/auth/sessions';
        var sessions = [];
        var session = {};
        var payload = {};
        session.username = tableau.username;
        session.password = tableau.password;
        session.caller = 'WEB';
        sessions[0] = session;
        payload.sessions = sessions;
        payload = JSON.stringify(payload);
        //console.log(payload);

        //sessions='{"sessions": [{"username": "USERNAME","password": "PASSWORD","caller": "WEB"}]}'
        $.ajax({
            url: url,
            contentType: 'application/vnd.alertme.zoo-6.1+json',
            headers: {
                'X-Omnia-Client': 'Hive Web Dashboard',
                'Accept': 'application/vnd.alertme.zoo-6.1+json'
            },
            method: 'POST',
            data: payload,
            success: function(data) {
                //console.log(data);
                resp = data;
                if (resp.code == 'Unknown') {
                    tableau.log("Error: " + resp.message);
                    tableau.abortWithError("Error: " + resp.message);
                    return;
                }
                var sessionId = resp.sessions[0].id;
                //console.log(sessionId);


                var url = 'https://api-prod.bgchprod.info:8443/omnia/nodes';

                $.ajax({
                    url: url,
                    contentType: 'application/vnd.alertme.zoo-6.1+json',
                    headers: {
                        'X-Omnia-Client': 'Hive Web Dashboard',
                        'Accept': 'application/vnd.alertme.zoo-6.1+json',
                        'X-Omnia-Access-Token': sessionId
                    },
                    success: function(data) {
                        //console.log (data);

                        //get schedules
                        nodes = data.nodes;
                        var heatingNodeId = connectionData.heatingId;
                        var hotwaterNodeId = connectionData.hotwaterId;


                        //if user wants hot water or heating schedule
                        if (table.tableInfo.id == 'hotwater' || table.tableInfo.id == 'heating') {
                            var node = {};
                            if (table.tableInfo.id == 'hotwater') {
                                id = hotwaterNodeId;
                            } else if (table.tableInfo.id == 'heating') {
                                id = heatingNodeId;
                            }


                            // Iterate over the JSON object
                            for (var i = 0, len = nodes.length; i < len; i++) {
                                if (nodes[i].id == id) {
                                    node = nodes[i];
                                }
                            }
                            var reportedValue = node.attributes.schedule.reportedValue;
                            var keys = Object.keys(reportedValue);
                            tableData = [];

                            // Iterate over the JSON object
                            for (var i = 0, len = keys.length; i <= len; i++) {
                                var weekday = keys[i];
                                var schedule = reportedValue[weekday];
                                //console.log (schedule);
                                for (var z = 0, len = schedule.length; z < len; z++) {

                                    tableData.push({
                                        "tableId": table.tableInfo.id,
                                        "deviceId": heatingNodeId,
                                        "time": schedule[z].time,
                                        "heatCoolMode": schedule[z].heatCoolMode,
                                        "targetHeatTemperature": schedule[z].targetHeatTemperature,
                                        "weekday": weekday
                                    });
                                }
                            }
                            table.appendRows(tableData);
                            doneCallback();
                            return;
                        }

                        if (table.tableInfo.id == 'temperature' || table.tableInfo.id == 'targetTemperature') {
                            //console.log(heatingNodeId);
                            endDate = new Date(connectionData.endDate);
                            startDate = new Date(connectionData.startDate); //start

                            var url = 'https://api-prod.bgchprod.info:8443/omnia/channels/temperature@' + heatingNodeId + ',targetTemperature@' + heatingNodeId + '?start=' + startDate.getTime() + '&end=' + endDate.getTime() + '&rate=5&timeUnit=MINUTES&operation=AVG';

                            $.ajax({
                                url: url,
                                contentType: 'application/vnd.alertme.zoo-6.1+json',
                                headers: {
                                    'X-Omnia-Client': 'Hive Web Dashboard',
                                    'Accept': 'application/vnd.alertme.zoo-6.1+json',
                                    'X-Omnia-Access-Token': sessionId
                                },
                                success: function(data) {
                                    //console.log(data);
                                    channels = data.channels;


                                    if (table.tableInfo.id == 'temperature') {
                                        channel = channels[0];
                                    } else if (table.tableInfo.id == 'targetTemperature') {
                                        channel = channels[1];
                                    }

                                    //console.log(channel.values);
                                    keys = Object.keys(channel.values);
                                    tableData = [];

                                    // Iterate over the JSON object
                                    for (var i = 0, len = keys.length; i < len; i++) {

                                        tableData.push({
                                            "tableId": table.tableInfo.id,
                                            "deviceId": heatingNodeId,
                                            "startTime": keys[i],
                                            "formattedStartTime": new Date(parseInt(keys[i])),
                                            "temperature": channel.values[keys[i]]
                                        });
                                    }

                                    table.appendRows(tableData);

                                    doneCallback();
                                }
                            })
                        }
                    }
                });

            }
        });
    };

    tableau.registerConnector(myConnector);


    // Create event listeners for when the user submits the form
    $(document).ready(function() {
        $("#submitButton").click(function() {

            tableau.password = document.getElementById('password').value;
            tableau.username = document.getElementById('username').value


            tableau.connectionName = "Hive Data Feed For " + tableau.username; // This will be the data source name in Tableau


            var url = 'https://beekeeper.hivehome.com/1.0/global/login';

            payload = '{"username":"' + tableau.username + '","password":"' + tableau.password + '","devices":true,"products":true}'
            //console.log(payload);

            $.ajax({
                url: url,
                //contentType: 'Content-Type:application/json',
                headers: {
                    'Content-Type': 'application/json',
                },
                method: 'POST',
                data: payload,
                success: function(data) {
                    //console.log(data)
                    var connectionData = {};
                    products = data.products;

                    for (var x = 0, len = products.length; x < len; x++) {
                        if (products[x].type == "hotwater") {
                            connectionData.hotwaterId = data.products[x].id;
                        } else if (products[x].type == "heating") {
                            connectionData.heatingId = data.products[x].id;
                        }
                    }


                    connectionData.endDate = document.getElementById("endDate").value;
                    connectionData.startDate = document.getElementById("startDate").value;
                    tableau.connectionData = JSON.stringify(connectionData);
                    tableau.submit(); // This sends the connector object to Tableau

                },
				error: function(xhr, status, error){
					//alert("Your username/password seems to be incorrect. Error: " + xhr.status);
					tableau.log("Your username/password seems to be incorrect. Error: " + xhr.status);
                    tableau.abortWithError("Your username/password seems to be incorrect. Error: " + xhr.status);
				}
            });
        });
    });
    // Init function for connector, called during every phase
    myConnector.init = function(initCallback) {
        tableau.authType = tableau.authTypeEnum.custom;
        initCallback();
    }
})();