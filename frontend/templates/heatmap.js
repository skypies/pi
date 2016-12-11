{{define "js-heatmap"}}

// Depends on "js-polling" and "js-textboxes"; abuses the 'notes' textbox.

// InitHeatmap();
// FetchAndPaintHeatmap(duration);                // One-off render
// PollForHeatmap(duration,interval,expirytime);  // redraw every 'interval' until 'expirytime'

// https://developers.google.com/maps/documentation/javascript/heatmaplayer

var heatmap = null;

var heatmapCoords;
var heatmapIcaoId = "";
var heatmapDurationAll;
var heatmapDurationSingle = "15m";
var heatmapInterval;
var heatmapIsPolling = false;

function getDuration() {
    duration = "14m";
    if (heatmapIcaoId != "") {
        duration = heatmapDurationSingle;
    } else {
        duration = heatmapDurationAll;
    }
    return duration;
}

function InitHeatmap() {
    heatmapCoords = new google.maps.MVCArray([]);
    heatmap = new google.maps.visualization.HeatmapLayer({data: heatmapCoords});
    heatmap.setMap(map);
    heatmapIsPolling = false;
}

function SetHeatmapIcaoId(icaoid) { heatmapIcaoId = icaoid }

function FetchAndPaintHeatmap(duration) {
    if (duration == "") { duration = getDuration() }

    fetchAndPaintNewHeatmap(duration, function(){
        PaintNotes(heatmapSummary());
    });
}

function PollForHeatmap(duration,intervalMillis,expires) {
    heatmapDurationAll = duration;
    heatmapInterval = intervalMillis;
    PaintPollingLabel();
    // Actual polling is triggered by toggleHeatmapPolling
}

function heatmapSummary() {
    var str = "Complaints in last "+getDuration()+": "+heatmapCoords.length;
    if (heatmapIcaoId != "") {
        str += "<br/>IcaoId: "+heatmapIcaoId;
    }
    return str;
}

function PaintPollingLabel() {
    var label = ""
    if (heatmapIsPolling) {
        label = heatmapSummary();
        label += '<br/><button onclick="toggleHeatmapPolling()">Reset</button>';
    } else {
        label = '<button onclick="toggleHeatmapPolling()">See Complaints</button>'
    }

    PaintNotes(label)
}

function toggleHeatmapPolling() {
    name = "complaintheatmap";
    if (heatmapIsPolling) {
        heatmapIsPolling = false;
        SetHeatmapIcaoId("");
        clearHeatmapCoords();
        StopPolling(name);

    } else {
        heatmapIsPolling = true;

        SetHeatmapIcaoId(CurrentlySelectedIcao());
        
        StartPolling( function(){
            fetchAndPaintNewHeatmap("19s", PaintPollingLabel);
        }, heatmapInterval, name);
    }
    PaintPollingLabel()
}

function clearHeatmapCoords() {
    while(heatmapCoords.getLength() > 0) heatmapCoords.pop(); // How to empty an MVC array
}

function fetchAndPaintNewHeatmap(duration, callback) {
    var url = "https://stop.jetnoise.net/heatmap?d=" + duration;

    if (heatmapIcaoId != "") {
        url += "&icaoid=" + heatmapIcaoId;
    }
    
    $.getJSON(url, function(arrayData){
        clearHeatmapCoords();
        $.each(arrayData, function(idx, obj){
            heatmapCoords.push(new google.maps.LatLng(obj.Lat,obj.Long));
        });
    }).done(function(data) {
        // Only create this when the getJSON is done.
        if (callback != null) {
            callback();
        }
    });
}

{{end}}
