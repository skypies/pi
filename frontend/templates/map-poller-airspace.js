{{define "js-map-poller-airspace"}}

function paintBox(name, htmlFrag) {
    var box = document.getElementById(name);
    var div = document.createElement('div');
    div.innerHTML = htmlFrag;

    // Delete prev contents
    while (box.hasChildNodes()) {
        box.removeChild(box.lastChild);
    }
    box.appendChild(div);

}
function paintDetails(htmlFrag)  { paintBox('details', htmlFrag) }
function paintLegend(htmlFrag) {
    paintBox('legend',  htmlFrag)
    attachOnClicksToBoxText()
}

function attachOnClicksToBoxText() {
    $(function() {
        $("#toggle").click(function(e) {
            console.log("toggle:"+gPollingPaused);
            e.preventDefault(); // if desired...
            togglePolling()
        });
    });
}

// http://stackoverflow.com/questions/8682622/using-setinterval-to-do-simplistic-continuos-polling
var sleep2 = time => new Promise(resolve => setTimeout(resolve, time))
var poll2 = (promiseFn, time) => promiseFn().then(
             sleep2(time).then(() => poll2(promiseFn, time)))
//poll(() => new Promise(() => console.log('Hello World!')), 1000)

var gPollingIntervalMillis = 2000;
var gMaxPollsRemain = (60000/gPollingIntervalMillis)*30;  // 30m
var gPollsRemain = gMaxPollsRemain;

function pollingOverlay() {
    paintLegend( generateLegend() )
    infowindow = new google.maps.InfoWindow({ content: "holding..." });
    paintSearchBox()
    // Start polling ...

    poll2(() => new Promise(() => pollAndPaint( {{.URLToPoll}} )), gPollingIntervalMillis)
}

var gAircraft = {};         // Current marker objects for all live aircraft (keyed on icaoID)
var gExpiredAircraft = {};  // Expired (red) markers kept for all eternity (keyed on icaoID)
var gInfowindowIcao24 = ""; // The ID of the aircraft which was last rendered into the infowindow

var gPollingPaused = false;
function togglePolling() {
    if (gPollingPaused) {
        gPollingPaused = false
        gPollsRemain = gMaxPollsRemain
    } else {
        gPollingPaused = true
    }
    paintLegend( generateLegend() )
}

function generateLegend() {
    var legend = ''
    if (gPollingPaused) { legend += '[<a href="#" id="toggle">Resume polling</a>]' }
    else                { legend += "<i>(Polling active)</i>" }

    var now = new Date();
    var tstamp = now.toTimeString()
    
    return legend+" "+tstamp
}

function pollAndPaint(url) {
    if (gPollingPaused) { return }
    paintLegend( generateLegend() )

    gPollsRemain--;
    if (gPollsRemain <= 0) {
        togglePolling();
        return;
    }
    
    var liveAircraft = {};
    $.getJSON( url, function( data ) {
        $.each( data["Aircraft"], function( icaoid, aircraft ) {
            paintAircraft(aircraft);
            liveAircraft[icaoid] = 1;

            if (gExpiredAircraft.hasOwnProperty(icaoid)) {
                // No longer expired ! Delete the red marker
                gExpiredAircraft[icaoid].setMap(null);
                delete gExpiredAircraft[icaoid];
            }
        });
        expireAircraft(liveAircraft);
        // paintDetails("live: "+Object.keys(liveAircraft).length+
        //           ", prev: "+Object.keys(gAircraft).length)
    });
}    

function expireAircraft(live) {
    for (k in gAircraft) {
        if (!k) continue;
        if (! live.hasOwnProperty(k)) {
            gExpiredAircraft[k] = gAircraft[k];
            delete gAircraft[k];
            gExpiredAircraft[k].setIcon(arrowicon("#ff0000",gExpiredAircraft[k].getIcon().rotation))
            // gExpiredAircraft[k].setMap(null);
        }
    }
}

// http://stackoverflow.com/questions/23045884/can-i-use-marshaljson-to-add-arbitrary-fields-to-a-json-encoding-in-golang
// {"Aircraft":
//  {"71BE21":
//   {"Msg": {"Type":"MSG"    (or "MLAT")
//            "Icao24":"71BE21",
//            "GeneratedTimestampUTC":"2016-11-14T19:46:10.72Z",
//            "Callsign":"KAL018",
//            "Altitude":17100,
//            "GroundSpeed":427,
//            "Track":312,
//            "Position":
//            {"Lat":34.21724, "Long":-119.3715},
//            "VerticalRate":1600,
//            "Squawk":"1320",
//            "ReceiverName":"CulverCity"
//           },
//    "Icao24":"71BE21",
//    "Registration":"HL7621",
//    "EquipmentType":"A388",
//    "CallsignPrefix":"KAL",
//    "Number":0,
//    "IATA":"",
//    "ICAO":"",
//    "PlannedDepartureUTC":"0001-01-01T00:00:00Z",
//    "PlannedArrivalUTC":"0001-01-01T00:00:00Z",
//    "ArrivalLocationName":"",
//    "DepartureLocationName":"",
//    "Origin":"",
//    "Destination":"",
//    "NumMessagesSeen":278,
//    "Source":""
//   },
//   {"ABC123": {...}}
//  }

var infowindow;     // This single object is the popup

function paintAircraft(a) {
    var flightnumber = "";
    if (a.IATA && a.Number) { flightnumber = a.IATA+a.Number }
    var ident = flightnumber;
    if (!ident) {
        ident = a.Msg.Callsign
    }
    if (!ident) {
        ident = a.Registration
    }
    var header = '<b>'+ident+'</b><br/>';
    if (a.X_UrlSkypi) {
        header = '<b><a target="_blank" href="'+a.X_UrlSkypi+'">'+ident+'</a></b> '+
            '[<a target="_blank" href="'+a.X_UrlFA+'">FA</a>,'+
            ' <a target="_blank" href="'+a.X_UrlFR24+'">FR24</a>,'+
            ' <a target="_blank" href="'+a.X_UrlDescent+'">Descent</a>'+
            ']<br/>'
    }

    var infostring = header +
        'FlightNumber: '+flightnumber+'<br/>'+
        'Schedule: '+a.Origin+" - "+a.Destination+'<br/>'+
        'Callsign: '+a.Msg.Callsign+'<br/>'+
        'Icao24: '+a.Icao24+'<br/>'+
        '  -- Registration: '+a.Registration+'<br/>'+
        '  -- IcaoPrefix: '+a.CallsignPrefix+'<br/>'+
        '  -- Equipment: '+a.EquipmentType+'<br/>'+
        'PressureAltitude: '+a.Msg.Altitude+' feet<br/>'+
        'GroundSpeed: '+a.Msg.GroundSpeed+' knots<br/>'+
        'Heading: '+a.Msg.Track+' degrees<br/>'+
        'Position: ('+a.Msg.Position.Lat+','+a.Msg.Position.Long+')<br/>'+
        // 'Last seen: ('+a.X_AgeSecs+'s ago) '+a.Msg.GeneratedTimestampUTC+'<br/>'+
        'Last seen: '+a.Msg.GeneratedTimestampUTC+'<br/>'+
        'Source: '+a.Source+'/'+a.Msg.ReceiverName+' ('+ a.X_DataSystem+')<br/>';

    infostring = '<div id="infowindow">'+infostring+'</div>'
    
    var zDepth = 3000;
    if (a.Source == "fr24") { zDepth = 2000 }
    var color = "#0033ff"; // SkyPi/ADSB color
    if (a.X_DataSystem == "MLAT") { color = "#508aff" }
    if (a.Source == "fa") { color = "#ff3300" }
    if (a.Source == "fr24") { color = "#00ff33" }

    var newicon = arrowicon(color, a.Msg.Track);
    var newpos = new google.maps.LatLng(a.Msg.Position.Lat, a.Msg.Position.Long);
    var oldmarker = gAircraft[a.Icao24]
    if (!oldmarker) {
        // New aircraft - create a fresh marker
        var marker = new google.maps.Marker({
            title: ident,
            callsign: a.Msg.Callsign,  // This is used for search
            html: infostring,
            position: newpos,
            zIndex: zDepth,
            icon: newicon,
            map: map
        });        
        marker.addListener('click', function(){
            infowindow.setContent(this.html),
            infowindow.open(map, this);
            gInfowindowIcao24 = a.Icao24
        });
        gAircraft[a.Icao24] = marker

    } else {
        // Update existing marker
        oldmarker.setPosition(newpos);
        oldmarker.setIcon(newicon);
        oldmarker.html = infostring;
        // If the infowindow is currently displaying this aircraft, update it
        if (gInfowindowIcao24 == a.Icao24) {
            infowindow.setContent(infostring);
        }
    }
}

function arrowicon(color,rotation) {
    return {
        path: google.maps.SymbolPath.FORWARD_CLOSED_ARROW,
        scale: 3,
        strokeColor: color,
        strokeWeight: 2,
        rotation: rotation,
    };
}

// Crappy client-side searchbox. Interates over the markers, looking for one with the same callsign
function paintSearchBox() {
    var html = '<div>'+
        '<form id="srchform">'+
        '<button type="submit">Search Callsign</button> '+
        '<input type="text" name="callsign" size="8"/>'+
        '</form>'+
        '</div>'
    paintDetails(html)

    $(function() {
        $('#srchform').on("submit",function(e) {
            e.preventDefault(); // cancel the actual submit
            var callsign = document.getElementById('srchform').elements.callsign.value;
            document.getElementById('srchform').elements.callsign.value = '';
            searchAndMaybeHighlight(callsign.toUpperCase());
        });
    });
}

function searchAndMaybeHighlight(callsign) {
    console.log('Search for "' + callsign + '"');
    for (k in gAircraft) {
        var marker = gAircraft[k];
        if (marker.callsign == callsign) {
            if (gInfowindowIcao24 != "") {
                infowindow.close();
            }
            infowindow.setContent(marker.html);
            infowindow.open(map, marker);
            gInfowindowIcao24 = k
        }
    }
}

{{end}}
