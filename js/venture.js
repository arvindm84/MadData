const CATEGORIES = {
    "coffee shop": ["coffee", "cafe", "espresso", "latte", "cappuccino"],
    "restaurant": ["restaurant", "food", "eat", "dining", "lunch", "dinner", "brunch"],
    "pharmacy": ["pharmacy", "drugstore", "prescriptions", "CVS", "Walgreens", "medicine"],
    "grocery store": ["grocery", "groceries", "supermarket", "produce", "Whole Foods", "Aldi"],
    "bar": ["bar", "drinks", "beer", "cocktails", "nightlife", "brewery", "pub"],
    "gym": ["gym", "fitness", "workout", "exercise", "yoga", "crossfit"],
    "late night food": ["late night", "2am", "midnight", "after hours", "open late"],
    "bakery": ["bakery", "bread", "pastry", "donuts", "croissant", "baked goods"],
    "convenience store": ["convenience", "corner store", "bodega", "7-eleven"],
    "coworking space": ["coworking", "cowork", "workspace", "WeWork", "shared office"],
    "daycare": ["daycare", "childcare", "nursery", "preschool", "kids"],
    "hardware store": ["hardware", "tools", "Home Depot", "lumber", "plumbing"],
    "urgent care": ["urgent care", "clinic", "walk-in", "emergency", "doctor"]
};

function categorize(input) {
    const text = input.toLowerCase();
    for (const [cat, keywords] of Object.entries(CATEGORIES)) {
        if (keywords.some(kw => text.includes(kw))) return cat;
    }
    return "general business";
}

const submitBtn = document.getElementById('venture-submit');
const inputField = document.getElementById('venture-input');
const resultsArea = document.getElementById('results-container');

if (submitBtn) {
    submitBtn.addEventListener('click', () => {
        const query = inputField.value.trim();
        if (!query) return;

        const category = categorize(query);
        resultsArea.innerHTML = `<div class="loading-state">Analyzing locations for <strong>${category}</strong>...</div>`;
        resultsArea.style.display = 'block';
        resultsArea.style.borderStyle = 'solid';

        fetch('../data/vacant_lots_scored.geojson')
            .then(res => res.json())
            .then(data => {
                const matches = data.features.map(f => {
                    const allScores = f.properties.all_scores_json || [];
                    const scoreData = allScores.find(s => s.category === category) ||
                        allScores.find(s => s.category === "general business") ||
                        { score: 0, reason: "Data unavailable for this location." };

                    const lng = f.geometry.coordinates[0].toFixed(4);
                    const lat = f.geometry.coordinates[1].toFixed(4);
                    const coords = `(${lat}, ${lng})`;
                    const baseAddr = f.properties['addr:street'] ? `${f.properties['addr:housenumber'] || ''} ${f.properties['addr:street']}` : "Madison Vacant Lot";

                    return {
                        address: `${baseAddr} ${coords}`,
                        lat: parseFloat(lat),
                        lng: parseFloat(lng),
                        score: scoreData.score,
                        reason: scoreData.reason,
                        id: f.properties.id
                    };
                });

                matches.sort((a, b) => b.score - a.score);
                const top5 = matches.slice(0, 5);

                renderResults(top5, category);
            })
            .catch(err => {
                resultsArea.innerHTML = `<p class="error">Error loading data. Please try again.</p>`;
            });
    });
}

function renderResults(results, category) {
    if (results.length === 0) {
        resultsArea.innerHTML = `<p>No suitable locations found for this category.</p>`;
        return;
    }

    let html = `
        <div class="results-header">
            <h2>Top 5 Locations for "${category.charAt(0).toUpperCase() + category.slice(1)}"</h2>
        </div>
        <div class="results-list">
    `;

    results.forEach((res, i) => {
        const colorClass = res.score >= 80 ? 'probability-high' : (res.score >= 50 ? 'probability-med' : 'probability-low');
        html += `
            <div class="result-item" onclick="window.location.href='map.html?lat=${res.lat}&lng=${res.lng}'" style="cursor: pointer;">
                <div class="result-rank">#${i + 1}</div>
                <div class="result-info">
                    <div class="result-address">${res.address}</div>
                    <div class="result-reason">${res.reason}</div>
                </div>
                <div class="result-score ${colorClass}">
                    <div class="score-num">${res.score}%</div>
                    <div class="score-label">Success Chance</div>
                </div>
            </div>
        `;
    });
    resultsArea.innerHTML = html;
}
