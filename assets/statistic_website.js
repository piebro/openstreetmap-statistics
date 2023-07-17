data_cache = {}
data_cache_list = []
max_cache_size = 30

function array_to_dict(array) {
    const dict = {};
    for (let i = 0; i < array.length; i++) {
        dict[array[i]] = i;
    }
    return dict;
}

function options_to_selection(select, keys){
    prev_value = select.value
    while (select.firstChild) {
        select.removeChild(select.lastChild)
    }
    for (key of keys){
        var opt = document.createElement('option')
        opt.value = key
        opt.innerHTML = key.replaceAll("_", " ")
        select.appendChild(opt)
    }
    if (keys.includes(prev_value)){
        select.value = prev_value
    }
}

async function load_data(data_name){
    if (data_name in data_cache){
        return data_cache[data_name]
    } else {
        data_cache[data_name] = await fetch("assets/data/" + data_name + ".json").then(response => {return response.json()})
        data_cache_list.push(data_name)
        if (data_cache_list.length > max_cache_size){
            delete data_cache[data_cache_list.shift()]
        }
        return data_cache[data_name]
    }
}

function add_div(id, class_str, parent_div=null){
    let div = document.createElement('div')
    div.id = String(id)
    div.style.display = "inline-block"
    div.classList.add(class_str);
    if (parent_div == null){
        document.getElementById('data').appendChild(div)
    } else {
        parent_div.appendChild(div)
    }
    return div
}

function add_text(text){
    show_content = async function(div_id){
        div = add_div(div_id, "textDiv")
        div.insertAdjacentHTML('beforeend', text);
    }
    return {"show": show_content}
}

function get_plot_config(){
    return {"displayModeBar": false, "staticPlot":(screen.width < 850)}
}

function get_plot_layout(){
    return {
        "font": {"family": "Times", "size": "15"},
        "paper_bgcolor": "#dfdfdf",
        "plot_bgcolor": "#dfdfdf",
    }
}

function get_line_plot_layout(plot_title, x_unit, y_unit, percent, bar_chart_on_top_of_each_other){
    layout = Object.assign({}, get_plot_layout(), {
        "margin": {"l": 55, "r": 55, "b": 55, "t": 55},
        "title": {"text": plot_title},
        "xaxis": {"title": {"text": x_unit}},
        "yaxis": {"title": {"text": y_unit}, "rangemode": "tozero"},
    })
    if (percent){
        layout["yaxis"]["range"] = [0, 101]
    }
    if (bar_chart_on_top_of_each_other){
        layout["barmode"] = "stack"
    }
    return  layout
}

function get_map_plot_layout(plot_title, max_z_value){
    colorscale = [
        [0, "rgba(255,255,255,0)"],
        [0.00000001, "rgb(12,51,131)"],
        [1 / 1000, "rgb(10,136,186)"],
        [1 / 100, "rgb(242,211,56)"],
        [1 / 10, "rgb(242,143,56)"],
        [1, "rgb(217,30,30)"],
    ]
    layout = Object.assign({}, get_plot_layout(), {
        "margin": {"l": 20, "r": 20, "b": 35, "t": 35},
        "images": [{
            "source": "assets/background_map.png",
            "xref": "x",
            "yref": "y",
            "x": 0,
            "y": 180,
            "sizex": 360,
            "sizey": 180,
            "sizing": "stretch",
            "opacity": 1,
            "layer": "below",
        }],
        "xaxis": {"showgrid": false, "visible": false},
        "yaxis": {"showgrid": false, "visible": false, "scaleanchor": "x", "scaleratio": 1},
        "coloraxis": {"colorscale": colorscale, "cmin": 0, "cmax": max_z_value},
        "title": {"text": plot_title},
    })
    return layout
}

function add_single_line_plot(plot_title, data_name, options={}){
    const default_options = {
        x_column_name: undefined,
        y_column_name: undefined,
        percent: false,
    };
    const opt = Object.assign({}, default_options, options);
    show_content = async function(div_id){
        data = await load_data(data_name)
        if (opt.x_column_name == undefined){opt.x_column_name = data["columns"][0]}
        if (opt.y_column_name == undefined){opt.y_column_name = data["columns"][1]}

        if (opt.percent) {
            y_unit = "%"
        } else {
            y_unit = opt.y_column_name
        }

        div = add_div(div_id, "dataDiv")
        const x = data["data"].map(tuple => tuple[data["columns"].indexOf(opt.x_column_name)]);
        const y = data["data"].map(tuple => tuple[data["columns"].indexOf(opt.y_column_name)]);
        traces = [{"x": x, "y": y, "mode": "lines", "name": "", "hovertemplate": "%{x}<br>%{y:,} " + y_unit}]
        Plotly.newPlot(div, traces, get_line_plot_layout(plot_title, opt.x_column_name, y_unit, opt.percent), get_plot_config())
    }
    return {"show": show_content, "save_plot": async function(div_id){save_plot(data_name, div_id)}, "save_data": async function(){save_data(data_name)}}
}

function add_multi_line_plot(plot_title, data_name, y_unit, options={}){
    const default_options = {
        x_column_name: undefined,
        y_column_names: undefined,
        percent: false,
        on_top_of_each_other: false,
        bar_chart: false,
        reverse_y_names: false,
    };
    const opt = Object.assign({}, default_options, options);

    show_content = async function(div_id){
        data = await load_data(data_name)
        if (opt.x_column_name == undefined){opt.x_column_name = data["columns"][0]}
        if (opt.y_column_names == undefined){opt.y_column_names = data["columns"].slice(1)}

        const x = data["data"].map(tuple => tuple[data["columns"].indexOf(opt.x_column_name)]);
        div = add_div(div_id, "dataDiv")
        traces = []
        for (y_column_name of opt.y_column_names){
            const y = data["data"].map(tuple => tuple[data["columns"].indexOf(y_column_name)]);
            trace = {
                "x": x,
                "y": y,
                "mode": "lines",
                "name": y_column_name,
                "hovertemplate": "%{x}<br>%{y:,} " + y_unit
            }
            if (opt.on_top_of_each_other){
                trace["stackgroup"] = "one"
            }
            if (opt.bar_chart){
                trace["type"] = "bar"
            }
            if (opt.reverse_y_names){
                traces.unshift(trace)
            } else {
                traces.push(trace)
            }
            
        }
        
        const bar_chart_on_top_of_each_other = opt.bar_chart && opt.on_top_of_each_other
        Plotly.newPlot(div, traces, get_line_plot_layout(plot_title, opt.x_column_name, y_unit, opt.percent, bar_chart_on_top_of_each_other), get_plot_config())
    }
    return {"show": show_content, "save_plot": async function(div_id){save_plot(data_name, div_id)}, "save_data": async function(){save_data(data_name)}}
}

function get_map_plot_traces(x, y, z, max_z_value){
    traces = [{
            "type": "histogram2d",
            "x": x,
            "y": y,
            "z": z,
            "zmax": max_z_value,
            "histfunc": "sum",
            "autobinx": false,
            "xbins": {"start": 0, "end": 360, "size": 1},
            "autobiny": false,
            "ybins": {"start": 0, "end": 180, "size": 1},
            "coloraxis": "coloraxis",
    }]
    return traces
}

function add_map_plot(plot_title, data_name){
    show_content = async function(div_id){
        data = await load_data(data_name)
        const data_indices = array_to_dict(data["columns"])
        d = data["data"][0]
        traces = get_map_plot_traces(d[data_indices["x"]], d[data_indices["y"]], d[data_indices["z"]], d[data_indices["max_z_value"]])

        div = add_div(div_id, "dataDiv")
        Plotly.newPlot(div, traces, get_map_plot_layout(plot_title, d[data_indices["max_z_value"]]), get_plot_config())
    }
    return {"show": show_content, "save_plot": async function(div_id){save_plot(data_name, div_id)}, "save_data": async function(){save_data(data_name)}}
}

function add_multiple_map_plots(data_name){
    show_content = async function(div_id){
        data = await load_data(data_name)
        const data_indices = array_to_dict(data["columns"])
        for (const d of data["data"]){
            div = add_div(d[data_indices["map_name"]], "dataDiv")
            traces = get_map_plot_traces(d[data_indices["x"]], d[data_indices["y"]], d[data_indices["z"]], d[data_indices["max_z_value"]])
            Plotly.newPlot(div, traces, get_map_plot_layout(d[data_indices["map_name"]], d[data_indices["max_z_value"]]), get_plot_config())
        }
    }
    save_map_plots = async function(div_id){
        data = await load_data(data_name)
        for (const d of data["data"]){
            map_name = d[data["columns"].indexOf("map_name")]
            save_plot(data_name + "_" + map_name.replaceAll(" ", "_"), map_name)
        }
    }
    return {"show": show_content, "save_plot": save_map_plots, "save_data": async function(){save_data(data_name)}}
}

function add_table(title, data_name, x_column_name, y_column_name, data_name_name_to_link=undefined){
    show_content = async function(div_id){
        data = await load_data(data_name)
        if (data_name_name_to_link !== undefined){
            name_to_link = await load_data(data_name_name_to_link)
        } else {
            name_to_link = {}
        }
        
        div = add_div(div_id, "dataDiv")
        div.style.width = "100%"
        div.style.aspectRatio = "auto"
        div.innerHTML = get_table_html(title, data, x_column_name, y_column_name, name_to_link)
    }
    return {"show": show_content, "save_data": async function(){save_data(data_name)}}
}

function get_table_html(title, data, x_column_name, y_column_name, name_to_link){
    const x_column_index = data.columns.indexOf(x_column_name)
    const head_entries = ["Rank", y_column_name].concat(data.data.map(list => list[x_column_index]))
    let head = "<tr>"
    for (var i in head_entries){
        if (i == 0){
            head += "<th>" + head_entries[i] + "</th>"
        } else {
            head += "<th class=\"sorting\" onclick=\"sort_table(event)\">" + head_entries[i] + "</th>"
        }
    }
    head += "</tr>"
    let body = ""
    let rank = 1
    for (let column_index in data.columns){
        if (column_index == x_column_index){
            continue
        }

        let name = data.columns[column_index]
        if (name in name_to_link){
            name = "<a href=" + name_to_link[name] + ">" + name + "</a>"
        }
        
        body += "<tr><td>" + String(rank) + "</td><td>" + name + "</td>"
        rank += 1

        for (y_value of data.data.map(list => list[column_index])){
            body += "<td>" + y_value.toLocaleString("en-US") + "</td>"
        }
    }
    return "<h3>" + title + "</h3><table style='margin: 0 auto;'>" + head + body + "</table>"
}


function download(data_str, title, file_ending){
    const downloadLink = document.createElement('a');
    downloadLink.download = title + "." + file_ending;
    if (file_ending == "csv"){
        data_str = window.URL.createObjectURL(new Blob([data_str], { type: 'text/csv' }))
    } else if (file_ending == "json"){
        data_str = "data:text/json;charset=utf-8," + encodeURIComponent(JSON.stringify(data_str))
    }
    downloadLink.href = data_str
    downloadLink.style.display = 'none';
    document.body.appendChild(downloadLink);
    downloadLink.click();
    document.body.removeChild(downloadLink);
}

async function save_plot(data_name, div_id){
    await Plotly.toImage(document.getElementById(div_id), {format:'png',height:500,width:1000}).then(function(png_str){
        download(png_str, data_name, "png")
    })
}


async function save_data(data_name){
    data = await load_data(data_name)
    download(data, data_name, "json")
}


function init(topics){
    selection_options = {}
    url_hash_to_topic_question = {}
    for (const [topic, topic_obj] of Object.entries(topics)) {
        selection_options[topic] = {}
        for (const [question, question_obj] of Object.entries(topic_obj)) {
            selection_options[topic][question] = {}
            url_hash_to_topic_question[question_obj["url_hash"]] = [topic, question]
        }
    }

    window.plausible = window.plausible || function() { (window.plausible.q = window.plausible.q || []).push(arguments) }

    
    update_select_0()
    if(window.location.hash) {
        url_hash = window.location.hash.substring(1)
        if (url_hash in url_hash_to_topic_question){
            topic = url_hash_to_topic_question[url_hash][0]
            question = url_hash_to_topic_question[url_hash][1]
            if (topic in selection_options && question in selection_options[topic]){
                document.getElementById('select_0').value = topic
                update_select_1()
                document.getElementById('select_1').value = question
            }
        }
    }
    update_content(topics, true)
    window.addEventListener('resize', function(event) {update_content(topics, false)}, true);
}

function update_select_0(){
    options_to_selection(document.getElementById('select_0'), Object.keys(selection_options))
    update_select_1()
}

function update_select_1(){
    current_select_0 = document.getElementById('select_0').value
    options_to_selection(document.getElementById('select_1'), Object.keys(selection_options[current_select_0]))
}

function on_change_select_0(topics){
    update_select_1()
    update_content(topics, true)
}

function on_change_select_1(topics){
    update_content(topics, true)
}

function get_current_select(){
    return [document.getElementById('select_0').value, document.getElementById('select_1').value]
}

async function update_content(topics, log_update){
    document.getElementById("data").innerHTML = ""  
    topic_question = get_current_select()
    topic = topic_question[0]
    question = topic_question[1]
    console.log(topic, question)
    if (log_update){
        plausible("Selected Plot(s)", {props: {"Topic Question": topic + ": " + question}})
    }

    for (const [index, content_functions] of topics[topic][question]["content_functions"].entries()){
        await content_functions["show"](index)
    }

    document.getElementById('save_plot_btn').onclick = async function(){
        for (const [index, content_functions] of topics[topic][question]["content_functions"].entries()){
            if ("save_plot" in content_functions){
                content_functions["save_plot"](index)
            }
        }
        plausible("Download Plot(s)", {props: {"Topic Question": topic + ": " + question}})
    }
    document.getElementById('save_data_btn').onclick = async function(){
        for (const [index, content_functions] of topics[topic][question]["content_functions"].entries()){
            if ("save_data" in content_functions){
                content_functions["save_data"](index)
            }
        }
        plausible("Download Data", {props: {"Topic Question": topic + ": " + question}})
    }
    history.replaceState(null, "", "#" + topics[topic][question]["url_hash"])
}

async function sort_table(e){
    for (var child=e.srcElement.parentElement.firstChild.nextSibling; child!==null; child=child.nextSibling) {
        if (child != e.srcElement && !child.classList.contains('sorting')){
            child.classList.remove('sorting_asc', 'sorting_desc');
            child.classList.add('sorting');
        }
    }

    if (e.srcElement.classList.contains('sorting')){
        e.srcElement.classList.remove('sorting');
        e.srcElement.classList.add('sorting_desc');
        var th = e.srcElement
        var ascending = false
    } else if (e.srcElement.classList.contains('sorting_desc')){
        e.srcElement.classList.remove('sorting_desc');
        e.srcElement.classList.add('sorting_asc');
        var th = e.srcElement
        var ascending = true
    } else {
        e.srcElement.classList.remove('sorting_asc');
        e.srcElement.classList.add('sorting');
        var th = e.srcElement.parentElement.lastChild
        var ascending = false
    }
    
    const getCellValue = (tr, idx) => tr.children[idx].innerText.replaceAll(",", "") || tr.children[idx].textContent.replaceAll(",", "");

    const comparer = (idx, asc) => (a, b) => ((v1, v2) => 
        v1 !== '' && v2 !== '' && !isNaN(v1) && !isNaN(v2) ? v1 - v2 : v1.toString().localeCompare(v2)
        )(getCellValue(asc ? a : b, idx), getCellValue(asc ? b : a, idx));

    const table = th.closest('table');
    Array.from(table.querySelectorAll('tr:nth-child(n+2)'))
        .sort(comparer(Array.from(th.parentNode.children).indexOf(th), ascending))
        .forEach(tr => table.appendChild(tr));
    
    for (var i in table.rows){
        if (i > 0){
            table.rows[i].cells[0].innerText = String(i)
        }
    }
}