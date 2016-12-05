{{define "js-heatmap"}}

// https://developers.google.com/maps/documentation/javascript/heatmaplayer

function FetchAndPaintHeatmap(duration) {
    if (duration == "") { duration = "15m" }

    var url = "https://stop.jetnoise.net/heatmap?d=" + duration;
    var coords = [];

    $.getJSON(url, function(arrayData){ 
        $.each(arrayData, function(idx, obj){
            coords.push(new google.maps.LatLng(obj.Lat,obj.Long));
        });
    }).done(function(data) {
        // Only create this when the getJSON is done.
        var heatmap = new google.maps.visualization.HeatmapLayer({
            data: coords
        });
        heatmap.setMap(map);
        PaintNotes("Heatmap: "+duration+"<br/>("+coords.length+" complaints)");
    });
}

{{end}}
