(function () {
    var mapEl = document.getElementById('map');
    if (!mapEl) return;

    var madison = [43.0760, -89.3900];
    var map = L.map('map', { zoomControl: false }).setView(madison, 14);

    L.control.zoom({ position: 'bottomright' }).addTo(map);

    L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
        attribution: '&copy; OpenStreetMap contributors &copy; CARTO',
        subdomains: 'abcd',
        maxZoom: 20
    }).addTo(map);

    fetch('../data/vacant_lots_scored.geojson')
        .then(response => response.json())
        .then(data => {
            L.geoJSON(data, {
                pointToLayer: function (feature, latlng) {
                    var dotIcon = L.divIcon({
                        className: 'desert-marker',
                        html: '<div class="purple-dot"></div>',
                        iconSize: [10, 10],
                        iconAnchor: [5, 5]
                    });
                    return L.marker(latlng, {
                        icon: dotIcon,
                        interactive: true,
                        bubblingMouseEvents: true
                    });
                },
                onEachFeature: function (feature, layer) {
                    var props = feature.properties || {};
                    var name = props.name || (props.building === 'retail' ? 'Vacant Retail Space' : 'Vacant Lot');
                    var address = [props['addr:housenumber'], props['addr:street']].filter(Boolean).join(' ');

                    var latlngStr = 'Coordinates unknown';
                    if (typeof layer.getBounds === 'function') {
                        var center = layer.getBounds().getCenter();
                        latlngStr = center.lat.toFixed(4) + ', ' + center.lng.toFixed(4);
                    } else if (typeof layer.getLatLng === 'function') {
                        var pt = layer.getLatLng();
                        latlngStr = pt.lat.toFixed(4) + ', ' + pt.lng.toFixed(4);
                    }

                    var popupContent = '<b>' + name + '</b>';
                    if (address) {
                        popupContent += '<br><i>' + address + '</i>';
                    }
                    popupContent += '<br><small style="color: rgba(0,0,0,0.5);">Coords: ' + latlngStr + '</small>';
                    layer.bindPopup(popupContent);

                    layer.on('click', function (e) {
                        var recsSection = document.getElementById('venture-recommendations');
                        var locName = document.getElementById('selected-location-name');

                        if (recsSection && locName) {
                            recsSection.classList.remove('hidden');
                            var displayContext = address ? address : '(' + latlngStr + ')';
                            locName.textContent = "Selected Location: " + displayContext;

                            // Update recommendations cards
                            var recs = props.top_recommendations_json || [];
                            var cards = document.querySelectorAll('.venture-card');

                            cards.forEach((card, index) => {
                                if (recs[index]) {
                                    card.style.display = 'flex';
                                    var typeEl = card.querySelector('.business-type');
                                    var probEl = card.querySelector('.chance-label span');
                                    var reasonEl = card.querySelector('.card-reason');

                                    var rec = recs[index];
                                    typeEl.textContent = rec.category.charAt(0).toUpperCase() + rec.category.slice(1);
                                    probEl.textContent = rec.score + "%";

                                    // Set probability color class
                                    probEl.className = '';
                                    if (rec.score >= 80) probEl.classList.add('probability-high');
                                    else if (rec.score >= 50) probEl.classList.add('probability-med');
                                    else probEl.classList.add('probability-low');

                                    reasonEl.textContent = rec.reason || "High demand area with low competition.";
                                } else {
                                    card.style.display = 'none';
                                }
                            });

                            setTimeout(function () {
                                recsSection.scrollIntoView({ behavior: 'smooth', block: 'start' });
                            }, 150);
                        }
                    });
                }
            }).addTo(map);

            // Deep-link handling: ?lat=...&lng=...
            const params = new URLSearchParams(window.location.search);
            const targetLat = parseFloat(params.get('lat'));
            const targetLng = parseFloat(params.get('lng'));

            if (!isNaN(targetLat) && !isNaN(targetLng)) {
                map.setView([targetLat, targetLng], 18);

                // Find the layer closest to target coordinates
                map.eachLayer(layer => {
                    if (layer.getLatLng) {
                        const latlng = layer.getLatLng();
                        // Tiny tolerance for float comparison
                        if (Math.abs(latlng.lat - targetLat) < 0.0001 &&
                            Math.abs(latlng.lng - targetLng) < 0.0001) {
                            setTimeout(() => {
                                layer.openPopup();
                                layer.fire('click');
                            }, 500);
                        }
                    }
                });
            }
        })
        .catch(error => console.error('Error loading GeoJSON:', error));

    map.on('popupclose', function () {
        var recsSection = document.getElementById('venture-recommendations');
        if (recsSection) {
            recsSection.classList.add('hidden');
        }
    });

    setTimeout(function () {
        map.invalidateSize();
    }, 500);
})();
