// giveaway.js

let currentPlatform = 'facebook';
let isRunning = false;
let isLiveMode = false;

document.addEventListener('DOMContentLoaded', () => {
    // Check if we are in live mode based on the presence of the page-selector
    const pageSelector = document.getElementById('page-selector');
    if (pageSelector) {
        isLiveMode = true;
        loadLivePages();
    } else {
        loadDemoPosts();
    }
});

function switchPlatform(platform) {
    currentPlatform = platform;
    if (isLiveMode) {
        // Facebook Graph API /me/accounts usually handles Pages mainly. 
        // For Instagram, we'd need a specific Instagram Business Account flow.
        // For now, reload pages if applicable or handle IG logic later.
        loadLivePages();
    } else {
        loadDemoPosts();
    }
}

async function loadLivePages() {
    const selector = document.getElementById('page-selector');
    if (!selector) return;

    selector.innerHTML = '<option value="" disabled selected>Loading Pages...</option>';

    try {
        const response = await fetch('/giveaways/api/live/pages', { credentials: 'include' });
        const data = await response.json();

        if (data.error) {
            selector.innerHTML = `<option value="" disabled>Error: ${data.error}</option>`;
            return;
        }

        selector.innerHTML = '<option value="" disabled selected>Select a Page...</option>';
        if (data.data && data.data.length > 0) {
            data.data.forEach(page => {
                const opt = document.createElement('option');
                opt.value = page.id;
                opt.textContent = page.name;
                selector.appendChild(opt);
            });
        } else {
            selector.innerHTML = '<option value="" disabled>No Pages found.</option>';
        }
    } catch (e) {
        console.error("Failed to load live pages", e);
        selector.innerHTML = '<option value="" disabled>Network Error loading pages</option>';
    }
}

// Global loadPagePosts function so it can be called from the HTML select onchange
window.loadPagePosts = async function (pageId) {
    const selector = document.getElementById('post-selector');
    selector.innerHTML = '<option value="" disabled>Loading Posts...</option>';

    try {
        const response = await fetch(`/giveaways/api/live/posts/${pageId}`, { credentials: 'include' });
        const data = await response.json();

        selector.innerHTML = ''; // Clear for multi-select

        if (data.data && data.data.length > 0) {
            data.data.forEach(post => {
                const opt = document.createElement('option');
                opt.value = post.id;
                const msg = post.message ? post.message.substring(0, 50) + "..." : "[No Text]";
                const dt = new Date(post.created_time).toLocaleDateString();
                opt.textContent = `${dt} - ${msg}`;
                selector.appendChild(opt);
            });
        } else {
            selector.innerHTML = '<option value="" disabled>No Posts found.</option>';
        }

    } catch (e) {
        console.error("Failed to load page posts", e);
        selector.innerHTML = '<option value="" disabled>Error loading posts</option>';
    }
}

async function loadDemoPosts() {
    const selector = document.getElementById('post-selector');
    if (!selector) return;

    selector.innerHTML = '<option value="" disabled>Loading...</option>';

    try {
        const response = await fetch('/giveaways/api/demo/posts');
        const posts = await response.json();

        selector.innerHTML = '';
        posts.filter(p => p.platform === currentPlatform).forEach(post => {
            const opt = document.createElement('option');
            opt.value = post.id;
            opt.textContent = `${post.date} - ${post.text.substring(0, 50)}...`;
            selector.appendChild(opt);
        });

    } catch (e) {
        console.error("Failed to load posts", e);
        selector.innerHTML = '<option value="" disabled>Error loading posts</option>';
    }
}

async function startDraw() {
    if (isRunning) return;

    const postSelector = document.getElementById('post-selector');

    // Get all selected options
    const selectedOptions = Array.from(postSelector.selectedOptions);
    const postIds = selectedOptions.map(opt => opt.value);

    if (postIds.length === 0 || postIds[0] === "") {
        alert("Veuillez sélectionner au moins un post.");
        return;
    }

    const numWinners = parseInt(document.getElementById('num_winners').value);

    const filters = {
        filter_duplicates: document.getElementById('filter_duplicates').checked,
        min_mentions: parseInt(document.getElementById('min_mentions').value),
        required_word: document.getElementById('required_word').value
    };

    // 1. Show Slot Machine Overlay
    const overlay = document.getElementById('slot-machine-overlay');
    const reel = document.getElementById('slot-reel');
    const title = document.getElementById('slot-title');

    overlay.style.setProperty('display', 'flex', 'important');
    title.textContent = "Téléchargement et filtrage des commentaires...";

    // Fill reel with random fake names for the spinning effect
    let fakeNamesHTML = '';
    for (let i = 0; i < 30; i++) {
        fakeNamesHTML += `<div class="slot-item">Analyse... participant #${Math.floor(Math.random() * 1000)}</div>`;
    }
    reel.innerHTML = fakeNamesHTML;

    // 2. Send API Request (Modify backend to accept list of post_ids)
    try {
        isRunning = true;
        const response = await fetch('/giveaways/api/draw', {
            method: 'POST',
            credentials: 'include',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                post_ids: postIds, // <--- MODIFIED to send array
                platform: currentPlatform,
                num_winners: numWinners,
                filters: filters,
                is_live: isLiveMode
            })
        });

        const data = await response.json();

        if (data.winners && data.winners.length > 0) {
            title.textContent = "SÉLECTION DU GAGNANT EN COURS...";
            await runSlotAnimation(data.winners);
            showResults(data.winners);
        } else {
            alert("Aucun participant ne correspond à vos filtres !");
            overlay.style.setProperty('display', 'none', 'important');
        }

    } catch (e) {
        console.error("Error during draw:", e);
        alert("Une erreur est survenue lors du tirage.");
        overlay.style.setProperty('display', 'none', 'important');
    } finally {
        isRunning = false;
    }
}

function runSlotAnimation(winners) {
    return new Promise(resolve => {
        const reel = document.getElementById('slot-reel');
        // Add fake names
        const names = ["Mohamed", "Sarra", "Ahmed", "Nermine", "Ali", "Fatma", "Walid", "Asma"];
        let html = '';
        for (let i = 0; i < 40; i++) {
            html += `<div class="slot-item text-muted">${names[Math.floor(Math.random() * names.length)]}</div>`;
        }

        // Ensure winner is at the very END of the reel so it stops on it
        // If multiple winners, we just stop on a general "GAGNANTS TROUVÉS" or the first one
        if (winners.length === 1) {
            html += `<div class="slot-item text-warning display-4">${winners[0].user_name} !!</div>`;
        } else {
            html += `<div class="slot-item text-warning display-4">${winners.length} GAGNANTS TROUVÉS !!</div>`;
        }

        reel.innerHTML = html;

        // Animation config
        const itemHeight = 120;
        const totalItems = reel.children.length;
        const stopPosition = (totalItems - 1) * itemHeight; // Stop at the last item

        // CSS Transition (Fast then slow)
        reel.style.transition = 'none';
        reel.style.transform = `translateY(0px)`;

        // Trigger reflow
        void reel.offsetWidth;

        // Spin for 4 seconds
        reel.style.transition = 'transform 4s cubic-bezier(0.15, 0.85, 0.25, 1)';
        reel.style.transform = `translateY(-${stopPosition}px)`;

        setTimeout(() => {
            resolve();
        }, 4500); // 4s animation + 0.5s pause to read the name
    });
}

function showResults(winners) {
    // Hide overlay
    document.getElementById('slot-machine-overlay').style.setProperty('display', 'none', 'important');

    // Show results panel
    const panel = document.getElementById('results-panel');
    const area = document.getElementById('winner-reveal-area');

    panel.style.display = 'block';
    area.innerHTML = '';

    // Fire Confetti!
    fireConfetti();

    // Build winner cards
    winners.forEach((w, index) => {
        setTimeout(() => {
            const card = document.createElement('div');
            card.className = "winner-card text-start";

            card.innerHTML = `
                <div class="d-flex align-items-center position-relative z-1">
                    <img src="${w.profile_pic_url}" class="winner-avatar me-4" alt="Avatar">
                    <div>
                        <div class="text-warning text-uppercase mb-1 fw-bold"><i class="bi bi-star-fill"></i> Gagnant #${index + 1}</div>
                        <h3 class="text-white mb-2">${w.user_name}</h3>
                        <div class="text-light fst-italic bg-dark p-2 border-start border-warning border-3 rounded">
                            "${w.text}"
                        </div>
                    </div>
                </div>
            `;

            area.appendChild(card);

            // Fire smaller confetti per winner if multiple
            if (index > 0) {
                confetti({
                    particleCount: 50,
                    spread: 60,
                    origin: { y: 0.8 }
                });
            }

        }, index * 800); // Stagger the appearance
    });
}

function fireConfetti() {
    var duration = 3 * 1000;
    var animationEnd = Date.now() + duration;
    var defaults = { startVelocity: 30, spread: 360, ticks: 60, zIndex: 99999 };

    function randomInRange(min, max) {
        return Math.random() * (max - min) + min;
    }

    var interval = setInterval(function () {
        var timeLeft = animationEnd - Date.now();

        if (timeLeft <= 0) {
            return clearInterval(interval);
        }

        var particleCount = 50 * (timeLeft / duration);
        // since particles fall down, start a bit higher than random
        confetti({
            ...defaults, particleCount,
            origin: { x: randomInRange(0.1, 0.3), y: Math.random() - 0.2 }
        });
        confetti({
            ...defaults, particleCount,
            origin: { x: randomInRange(0.7, 0.9), y: Math.random() - 0.2 }
        });
    }, 250);
}
