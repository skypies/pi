{{define "js-map-poller"}}

{{template "js-overlays"}}
{{template "js-textboxes"}}
{{template "js-airspace"}}

{{template "js-heatmap"}}

var map;
function initMap() {
    map = new google.maps.Map(document.getElementById('map'), {
        center: {lat: {{.Center.Lat}}, lng: {{.Center.Long}}},
        mapTypeId: google.maps.MapTypeId.TERRAIN,
        zoom: {{.Zoom}}
    });

    map.controls[google.maps.ControlPosition.RIGHT_TOP].push(
        document.getElementById('legend'));
    map.controls[google.maps.ControlPosition.LEFT_BOTTOM].push(
        document.getElementById('details'));

    ClassBOverlay();
    PathsOverlay();
    FetchAndPaintHeatmap("2m");
    
    InitMapsAirspace();
    poll2(() => new Promise(() => pollAndPaint( {{.URLToPoll}} )), gPollingIntervalMillis)
}

function PaintPollingLegend() {
    PaintLegend( generateLegend() );
    attachOnClicksToLegendText();  // Kinda hacky
}

// Any/all onClick events tagged to hyperlinks in the legend should be set here.
function attachOnClicksToLegendText() {
    $(function() {
        $("#toggle").click(function(e) {
            e.preventDefault();
            togglePolling()
        });
    });
}

function generateLegend() {
    var legend = ''
    if (gPollingPaused) { legend += '[<a href="#" id="toggle">Resume polling</a>]' }
    else                { legend += "<i>(Polling active)</i>" }

    var now = new Date();
    legend = legend + " " + CountVisibleAircraft() + " aircraft, " + now.toTimeString();

    return legend
}

// http://stackoverflow.com/questions/8682622/using-setinterval-to-do-simplistic-continuos-polling
var sleep2 = time => new Promise(resolve => setTimeout(resolve, time))
var poll2 = (promiseFn, time) => promiseFn().then(
             sleep2(time).then(() => poll2(promiseFn, time)))
//poll(() => new Promise(() => console.log('Hello World!')), 1000)


var gPollingIntervalMillis = 1000;
var gMaxPollsRemain = (60000/gPollingIntervalMillis)*30;  // 30m
var gPollsRemain = gMaxPollsRemain;

var gPollingPaused = false;
function togglePolling() {
    if (gPollingPaused) {
        gPollingPaused = false;
        gPollsRemain = gMaxPollsRemain;
    } else {
        gPollingPaused = true;
    }
    PaintPollingLegend();
}

function pollAndPaint(url) {
    if (gPollingPaused) { return }
    PaintPollingLegend();

    gPollsRemain--;
    if (gPollsRemain <= 0) {
        togglePolling();
        return;
    }
    
    var liveAircraft = {};
    $.getJSON( url, function( data ) {
        $.each( data["Aircraft"], function( icaoid, aircraft ) {
            PaintAircraft(aircraft);
            liveAircraft[icaoid] = 1;
        });
        ExpireAircraft(liveAircraft);
    });
}    

{{end}}
