var interval_id;
var cluster_data;

$(document).ready(function(){
    get_data();
    interval_id = window.setInterval(get_data, 3000);
    $("#update").click(get_data)
    $("#auto-update").click(function(){
        interval_id = window.setInterval(get_data, 3000);
    })
    $("#stop-update").click(function(){
        window.clearInterval(interval_id);
    })
    remove_blank(document.getElementById("head-line"));

    // listen scroll to make nav bar at top.
    window.addEventListener('scroll', onScroll);
})

function remove_blank(oEelement){
    for(var i=0;i<oEelement.childNodes.length;i++){
        var node=oEelement.childNodes[i];
        if(node.nodeType==3 && !/\S/.test(node.nodeValue)){
            node.parentNode.removeChild(node)
        }
    }
}


function get_data(){
    $.ajax({
        url:"/get-status",
        type:"GET",
        contentType: "application/json",
        success: create_page,
        timeout:2000
    })
}

function create_page(data){
    // teamup link
    // $("#teamup_link").attr('href', "https://teamup.com/" + data.teamup_id);

    // add the calendar date in the head line
    cluster_data = data;
    if (data.calendar_status){
        var schedule = $("#head-line .colum.schedule")
        schedule.empty()
        for (var i=0;i<data.date_list.length; i++) {
            schedule.append($("<div></div>").addClass("head colum schedule-day sample").text(data.date_list[i]))
        }
    }
    
    var content = $("#content-status").empty();
    for (var i=0; i<data.Nodes.length; i++) {
        var n_data = data.Nodes[i];
        var node = $("#content-status-sample .node-line").clone();
        // node information
        node.find(".node-name").text(n_data.hostname);
        node.find(".node-status").attr("data-status", n_data.status);
        node.find('.node-version').text(n_data.version);
        if (n_data.ips) {
            ips = n_data.ips;
            var ip_str = '';
            for (j = 0; j< ips.length; j++) {
                ip_str = ip_str + ips[j][1] + '(' + ips[j][0] + ')&nbsp;&nbsp;&nbsp;&nbsp;';
            }
            node.find('.node-ip').html(ip_str);
        }

        // fill gpu status
        var gpu_area = node.find(".gpu-list").empty();
        for (j=0; j<n_data.gpus.length; j++) {
            var gpu_data = n_data.gpus[j];
            var gpu_line = $("<div></div>").addClass("gpu-line");
            gpu_line.append($("<div></div>").addClass("colum gpu-idx").text(gpu_data.index))
            //memory
            gpu_line.append($("<div></div>").addClass("colum memory").text(gpu_data.use_mem + "/" + gpu_data.tot_mem));
            var mem_per = gpu_data.use_mem / gpu_data.tot_mem * 100;
            // console.log(gpu_line.find(".colum.memory"));
            gpu_line.find(".colum.memory").css("background", `linear-gradient(to right, #99CC66 ${mem_per}%, white ${mem_per}%, white)`);
            gpu_line.append($("<div></div>").addClass("colum utilize").text(gpu_data.utilize + " %"));
            // add current user information
            // update 2011.11.1
            //gpu_line.append($("<div></div>").addClass("colum users").text(gpu_data.users.map(x => x[0]).join(" ")));
            gpu_line.append($("<div></div>").addClass("colum users").text(' '));
            if (gpu_data.users.length==0) {
                gpu_line.find(".colum.users").html("&nbsp;")
            }
            
            else {
                html_str = ''
                for (ui=0; ui<gpu_data.users.length; ui++){
                    var u_info = gpu_data.users[ui];
                    if (u_info.user_code==0) { // legal user
                        html_str = html_str + u_info.username + " "
                    }
                    else { // illegal user
                        html_str = html_str + "<span class='illegal_user'>" + u_info.username + "</span>" + " "
                    }
                }
                gpu_line.find(".colum.users").html('<div>' + html_str + '</div>')
            }
            
            // add calendar
            if (data.calendar_status & "calendar" in gpu_data) {
                var calendar = $("<div></div>").addClass("colum schedule");
                gpu_line.append(calendar);
                gpu_data.calendar.map(
                    x => calendar.append(
                    $("<div></div>")
                    .addClass("colum schedule-day")
                    .html(
                        x.length > 0 ?x.map(ele => get_one_booking_html(ele)).join("<br>"): "&nbsp;")
                    ));
            }
            // update 2022.7.25
            var wrap_line = $("<div></div>");
            wrap_line.append(gpu_line);
            gpu_area.append(wrap_line);
        }

        content.append(node);

    }
    add_warning(data);
}

function add_warning(data){
    var warn_div = $("#warning");
    var illegal_user = $("<div></div>");
    illegal_user.html("<b>Users without booking (marked <span class='illegal_user'>orange</span>): </b>" + "<span class='blue'>" + data.illegal_users.join(" ") + "</span>" + " (by username string match)");
    warn_div.empty().append(illegal_user);
    warn_div.append(
        $("<div></div>").addClass('inline')
        .append(get_error_legend('illegal_booking', 'Invalid booking title(correct example: panda(cat))'))
        .append(get_error_legend('illegal_maxgpu', 'Exceed maximum gpus(4)'))
        .append(get_error_legend('illegal_maxday', 'Exceed maximum days(3) per gpu'))
    )
}

function get_error_legend(error_cls, error_txt){
    legend = $("<div></div>").addClass("legend_line inline")
    legend.append($("<div></div>").addClass("error_legend " + error_cls + " inline"))
    legend.append($("<span></span>").text(error_txt))
    return legend
}

function get_one_booking_html(booking){
    title = booking[0];
    who = booking[1];
    error_code = booking[2];
    var bk_html = title + '(' + who + ')';
    if (error_code == 1){
        bk_html = "<span class='illegal_booking'>" + bk_html + "</span>";
    }
    else if (error_code == 2){
        bk_html = "<span class='illegal_maxgpu'>"+ bk_html + "</span>";
    }
    else if (error_code == 3){
        bk_html = "<span class='illegal_maxday'>"+ bk_html + "</span>";
    }
    return bk_html
}

function onScroll(){
    var scrollTop = document.body.scrollTop || document.documentElement.scrollTop;
    if (scrollTop <= 345.6) {
        $("#head-line").removeClass();
        // console.log('scroll ' + String(scrollTop));
    }
    else {
        var scrollLeft = document.body.scrollLeft || document.documentElement.scrollLeft;
        $("#head-line").removeClass();
        $("#head-line").addClass("nav-at-top");
        var body_margin = parseInt($("body").css("margin-left"));
        $("#head-line").css("left", body_margin-scrollLeft)
        // console.log('scroll ' + String(scrollTop));
    }
}