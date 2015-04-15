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
<tr>
    <td>{{.Task}}</td>
    <td>{{.State}}</td>
    <td>{{.Success}}</td>
    <td>{{.Result}}</td>
    <td>{{.Enqueued}}</td>
    <td>{{.Started}}</td>
    <td>{{.Done}}</td>
    <td>
        {{if eq .State "done"}}
            <form action="/{{.ID}}/delete" method="POST" data-confirm="Are you sure you want to DELETE this job?"><button type="submit">delete</button></form>
            {{if not .Success}}
                <form action="/{{.ID}}/retry" method="POST" data-confirm="Are you sure you want to REQUEUE this job?"><button type="submit">retry</button></form>
            {{end}}
        {{end}}
    </td>
</tr>
<tr>
    <td colspan="7">{{printf "%#v" .Params}}</td>
</tr>
{{end}}

</table>
<script>
var confirmation = function(sel, msg) {
    var els = document.querySelectorAll(sel);
    for (var i = 0, len = els.length; i < len; ++i) {
        (function(el) {
            el.addEventListener("submit", function(ev) {
                var msg = el.getAttribute("data-confirm");
                if (!confirm(msg)) {
                    ev.preventDefault();
                }
            }, false);
        }(els[i]));
    }
};
confirmation("[data-confirm]");
</script>
</body>
</html>
`
)
