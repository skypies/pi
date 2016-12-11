{{define "js-polling"}}

// Routines to make adding AJAX pollers easy.

// StartPolling( function(){
//        console.log("Hi ho!");
//   }, 5000, "hiho");
//
// StopPolling("hiho");

// http://stackoverflow.com/questions/8682622/using-setinterval-to-do-simplistic-continuos-polling

var pleaseStopFlags = [];

function StopPolling(name) { pleaseStopFlags[name] = true }

function StartPolling(func, intervalMillis, name) {
    delete pleaseStopFlags[name];
    var SleepUntil = time => new Promise(resolve => setTimeout(resolve, time))
    var LoopUntil = (promiseFn, time, name) => promiseFn().then(
        SleepUntil(time).then(
            function() {
                if (! pleaseStopFlags[name]) {
                    LoopUntil(promiseFn, time, name);
                }
            }
        )
    )

    LoopUntil( () => new Promise(()=>func()), intervalMillis, name )
    //poll(() => new Promise(() => console.log('Hello World!')), 1000)
}

{{end}}
