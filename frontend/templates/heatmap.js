{{define "js-heatmap"}}

// Depends on "js-polling" and "js-textboxes"; abuses the 'notes' textbox.

// InitHeatmap();
// FetchAndPaintHeatmap(duration);                // One-off render
// PollForHeatmap(duration,interval,expirytime);  // redraw every 'interval' until 'expirytime'

// https://developers.google.com/maps/documentation/javascript/heatmaplayer

var heatmap = null;

var heatmapCoords;
var heatmapIcaoId = "";
var heatmapDuration;
var heatmapInterval;
var heatmapIsPolling = false;

function InitHeatmap() {
    heatmapCoords = new google.maps.MVCArray([]);
    heatmap = new google.maps.visualization.HeatmapLayer({data: heatmapCoords});
    heatmap.setMap(map);
    heatmapIsPolling = false;
}

function SetHeatmapIcaoId(icaoid) {
    heatmapIcaoId = icaoid;
    if (icaoid != "") {
        heatmapDuration = "15m";
    }
} // Bleargh

function FetchAndPaintHeatmap(duration) {
    if (duration == "") { duration = "15m" }
    heatmapDuration = duration

    fetchAndPaintNewHeatmap(function(){
        PaintNotes(heatmapSummary());
    });
}

function PollForHeatmap(duration,intervalMillis,expires) {
    heatmapDuration = duration;
    heatmapInterval = intervalMillis;
    PaintPollingLabel();
    // Actual polling is triggered by toggleHeatmapPolling
}

function heatmapSummary() {
    var str = "Complaints in last "+heatmapDuration+": "+heatmapCoords.length;
    if (heatmapIcaoId != "") {
        str += "<br/>IcaoId: "+heatmapIcaoId;
    }
    return str;
}

function PaintPollingLabel() {
    var label = ""
    if (heatmapIsPolling) {
        label = '';//'<button onclick="toggleHeatmapPolling()">Stop Heatmap</button><br/>';
        label += heatmapSummary();
    } else {
        label = '<button onclick="toggleHeatmapPolling()">See Complaints</button>'
    }

    PaintNotes(label)
}

function toggleHeatmapPolling() {
    if (heatmapIsPolling) {
        heatmapIsPolling = false;
    } else {
        console.log("engage heatmap!")
        heatmapIsPolling = true;

        StartPolling( function(){
            fetchAndPaintNewHeatmap(PaintPollingLabel);
        }, heatmapInterval);
    }
    PaintPollingLabel()
}

function fetchAndPaintNewHeatmap(callback) {
    var url = "https://stop.jetnoise.net/heatmap?d=" + heatmapDuration;

    if (heatmapIcaoId != "") {
        url += "&icaoid=" + heatmapIcaoId;
    }
    
    $.getJSON(url, function(arrayData){
        while(heatmapCoords.getLength() > 0) heatmapCoords.pop(); // How to empty an MVC array
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
