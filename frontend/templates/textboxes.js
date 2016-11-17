{{define "js-textboxes"}}

// The box names are defined in the HTML setup pages for each view.
//  legend: top-right
//  details: bottom-left

function PaintDetails(htmlFrag)  { PaintBox('details', htmlFrag) }
function PaintLegend(htmlFrag)   { PaintBox('legend',  htmlFrag) }

function PaintBox(name, htmlFrag) {
    var box = document.getElementById(name);
    var div = document.createElement('div');
    div.innerHTML = htmlFrag;

    // Delete prev contents
    while (box.hasChildNodes()) {
        box.removeChild(box.lastChild);
    }
    box.appendChild(div);

}

{{end}}
