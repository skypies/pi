{{define "js-polling"}}

// Routines to make adding AJAX pollers easy.


// http://stackoverflow.com/questions/8682622/using-setinterval-to-do-simplistic-continuos-polling

function StartPolling(func, millis) {
    var SleepUntil = time => new Promise(resolve => setTimeout(resolve, time))
    var LoopUntil = (promiseFn, time, expires) => promiseFn().then(
        SleepUntil(time).then(() => LoopUntil(promiseFn, time, expires)))

    LoopUntil( () => new Promise(()=>func()), millis )
    //poll(() => new Promise(() => console.log('Hello World!')), 1000)
}

// StartPolling( function(){
//        console.log("Hi ho!");
//    }, 1000);

{{end}}
