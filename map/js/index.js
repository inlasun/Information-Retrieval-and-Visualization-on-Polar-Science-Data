$(document).on('ready', function(){
    "use strict";
    var width = 960;
    var height = 960;
    var projection = d3.geo.mercator().scale(
            (width + 1) / 2 / Math.PI
        ).translate(
            [width / 2, height / 2]
        ).precision(0.1);
    var formatNumber = d3.format(",.0f");

    var color = d3.scale.category10();
    var graticule = d3.geo.graticule();

    var place_json = $.ajax({
        type: 'GET',
        url: '../map/json/full_places.json',
        dataType: 'json',
        success: function() {
            var places = place_json.responseJSON;
            var radius = d3.scale.sqrt().domain([0, 1e4]).range([0, 15]);
            loadD3(radius, places);
        }
    });

    var path = d3.geo.path().projection(projection);

    var svg = d3.select("body").append("svg").attr(
        "width", width
    ).attr(
        "height", height
    );

    svg.append("path").datum(graticule).attr(
        "class", "graticule"
    ).attr(
        "d", path
    );

    svg.append("path").datum(graticule.outline).attr(
        "class", "graticule outline"
    ).attr(
        "d", path
    );

    var loadD3 = function(radius, places) {
        //draw the bubble
        svg.selectAll(".pin").data(places).enter().append(
            "circle", ".pin"
        ).sort(
            function(a, b) {
                return b.occurrence - a.occurrence;
            }
        ).attr(
            "class", "bubble"
        ).attr("transform", function(d) {
            if(Math.abs(d.coordinate.latitude) == 90) {
                return;
            }
            return "translate(" + projection([
              d.coordinate.longitude,
              d.coordinate.latitude
            ]) + ")";
        }).attr(
            "r", function(d) {
                return radius(d.occurrence);
            }
        ).append("title").text(
            function(d) {
                return d.name + "\nOccurrence " + formatNumber(d.occurrence);
            }
        );

        //d3 map itself.
        d3.json("../map/json/readme-world.json", function(error, world) {
            var countries = topojson.feature(world, world.objects.countries).features;
            var neighbors = topojson.neighbors(world.objects.countries.geometries);

            svg.selectAll(".country").data(countries).enter().insert(
                "path", ".graticule"
            ).attr(
                "class", "country"
            ).attr(
                "d", path
            ).style(
                "fill",
                function(d, i) {
                    return color(
                        d.color = d3.max(
                            neighbors[i],
                            function(n) {
                                return countries[n].color;
                            }
                        ) + 1 | 0
                    );
                }
            );
        });
        d3.select(self.frameElement).style("height", height + "px");
    };
});
