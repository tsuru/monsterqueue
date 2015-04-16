// Copyright 2015 monsterqueue authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

const (
	indexTemplateData = `
<!DOCTYPE html>
<html>
<head>
    <title>MonsterWEB</title>
<style>
.extra {
    display: none;
}
.line {
    cursor: pointer;
}
.line:nth-child(4n + 1) {
    background: #f2f2f2;
}
</style>
</head>
<body>

<table>

<thead>
    <th>Task</th>
    <th>State</th>
    <th>Success</th>
    <th>Result</th>
    <th>Created</th>
    <th>Started</th>
    <th>Done</th>
</thead>

{{range .}}
<tr class="line">
    <td>{{.Task}}</td>
    <td>{{.State}}</td>
    <td>{{.Success}}</td>
    <td>{{.Result}}</td>
    <td>{{.Enqueued}}</td>
    <td>{{.Started}}</td>
    <td>{{.Done}}</td>
</tr>
<tr class="extra">
    <td colspan="7">
        {{if eq .State "done"}}
            <form action="/{{.ID}}/delete" method="POST" data-confirm="Are you sure you want to DELETE this job?"><button type="submit">delete</button></form>
            {{if not .Success}}
                <form action="/{{.ID}}/retry" method="POST" data-confirm="Are you sure you want to REQUEUE this job?"><button type="submit">retry</button></form>
            {{end}}
        {{end}}
        <div class="params">{{printf "%#v" .Params}}</div>
        <div class="stack"><pre>{{.Stack}}</pre></div>
    </td>
</tr>
{{end}}

</table>
<script src="https://ajax.googleapis.com/ajax/libs/jquery/2.1.3/jquery.min.js"></script>
<script>
$('body').on('click', '[data-confirm]', function(ev) {
    var el = $(ev.currentTarget);
    var msg = el.data('confirm');
    if (!confirm(msg)) {
        ev.preventDefault();
    }
});

$('body').on('click', '.line', function(ev) {
    var el = $(ev.currentTarget);
    el.next('.extra').toggle();
});
</script>
</body>
</html>
`
)
