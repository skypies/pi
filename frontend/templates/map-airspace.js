{{define "js-map-airspace"}} // Depends on: .Center (geo.Latlong), and .Zoom (int)

function localOverlay() {
    var legend = document.getElementById('legend');
    var div = document.createElement('div');
    div.innerHTML = {{.Legend}};
    legend.appendChild(div);

    var aircraft = {{.AircraftJS}}
    var infowindow = new google.maps.InfoWindow({ content: "holding..." });

    for (var i in aircraft) {
        var a = aircraft[i]
        var infostring =
            '<div><b><a target="_blank" href="'+a.fdburl+'">'+a.callsign+'</a></b><br/>'+
            '[<a target="_blank" href="'+a.faurl+'">FA</a>,'+
            ' <a target="_blank" href="'+a.fdburl+'&fr24=1">ADSB+fr24</a>,'+
            ' <a target="_blank" href="'+a.approachurl+'">ApproachGraph</a>'+
            ']<br/>'+
            'Icao24: '+a.icao24+'<br/>'+
            '  -- Registration: '+a.reg+'<br/>'+
            '  -- IcaoPrefix: '+a.icao+'<br/>'+
            '  -- Equipment: '+a.equip+'<br/>'+
            'Altitude: '+a.alt+' feet<br/>'+
            'Speed: '+a.speed+' knots<br/>'+
            'Heading: '+a.heading+' degrees<br/>'+
            'Position: ('+a.pos.lat+','+a.pos.lng+')<br/>'+
            'Last seen: '+a.age+' seconds ago<br/>'+
            'Source: '+a.source+' / '+a.receiver+'<br/>'
            '</div>';

        var marker = new google.maps.Marker({
            title: a.callsign,
            html: infostring,
            position: a.pos,
            icon: {
                path: google.maps.SymbolPath.FORWARD_CLOSED_ARROW,
                scale: 3,
                strokeColor: a.color,
                strokeWeight: 2,
                rotation: a.heading,
            },
            map: map
        });
        marker.addListener('click', function(){
            infowindow.setContent(this.html),
            infowindow.open(map, this);
        });
    }
}

{{end}}
