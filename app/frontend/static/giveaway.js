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
        const postGrid = document.getElementById('post-grid');
        if (postGrid) {
            postGrid.innerHTML = '<div class="text-muted text-center p-4">Sélectionnez d\'abord une page ci-dessus...</div>';
        }
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
        const response = await fetch(`/giveaways/api/live/pages?platform=${currentPlatform}`, { credentials: 'include' });
        const data = await response.json();

        if (data.error) {
            selector.innerHTML = `<option value="" disabled>Error: ${data.error.message || data.error}</option>`;
            return;
        }

        selector.innerHTML = '<option value="" disabled selected>Select a Page...</option>';
        if (data.data && data.data.length > 0) {
            data.data.forEach(page => {
                const opt = document.createElement('option');
                opt.value = page.id;
                opt.dataset.token = page.access_token;
                opt.textContent = page.name;
                selector.appendChild(opt);
            });
        } else {
            selector.innerHTML = `<option value="" disabled>No ${currentPlatform === 'instagram' ? 'Instagram Accounts connected to a Page' : 'Pages'} found.</option>`;
        }
    } catch (e) {
        console.error("Failed to load live pages", e);
        selector.innerHTML = '<option value="" disabled>Network Error loading pages</option>';
    }
}

// Global loadPagePosts function so it can be called from the HTML select onchange
window.loadPagePosts = async function (pageId, afterCursor = null) {
    const selector = document.getElementById('post-grid');
    const pageSelector = document.getElementById('page-selector');
    const selectedPageOption = pageSelector.options[pageSelector.selectedIndex];
    const pageToken = selectedPageOption ? selectedPageOption.dataset.token : null;

    const oldBtn = document.getElementById('load-more-btn');
    if (oldBtn) oldBtn.remove();

    if (!afterCursor) {
        selector.innerHTML = '<div class="text-warning text-center p-4 w-100 col-12"><i class="bi bi-arrow-repeat spin"></i> Chargement des Posts...</div>';
    } else {
        const loadingDiv = document.createElement('div');
        loadingDiv.id = 'loading-more';
        loadingDiv.className = 'text-warning text-center p-4 w-100 col-12';
        loadingDiv.innerHTML = '<i class="bi bi-arrow-repeat spin"></i> Chargement supplémentaire...';
        selector.appendChild(loadingDiv);
    }

    try {
        let url = `/giveaways/api/live/posts/${pageId}?platform=${currentPlatform}`;
        if (pageToken) {
            url += `&page_token=${encodeURIComponent(pageToken)}`;
        }
        if (afterCursor) {
            url += `&after=${encodeURIComponent(afterCursor)}`;
        }

        const response = await fetch(url, { credentials: 'include' });
        const data = await response.json();

        if (!afterCursor) selector.innerHTML = '';
        const loader = document.getElementById('loading-more');
        if (loader) loader.remove();

        if (data.error) {
            selector.innerHTML += `<div class="text-danger p-3 w-100 col-12">Erreur: ${data.error.message || data.error}</div>`;
            return;
        }

        if (data.data && data.data.length > 0) {
            data.data.forEach(post => renderPostCard(post, selector));

            // Generate Pagination Button if available
            if (data.paging && data.paging.cursors && data.paging.cursors.after) {
                const loadMoreBtn = document.createElement('button');
                loadMoreBtn.id = 'load-more-btn';
                loadMoreBtn.className = 'btn btn-outline-warning w-100 mt-3 col-12';
                loadMoreBtn.innerHTML = '<i class="bi bi-arrow-down-circle"></i> Charger plus d\'anciennes publications...';
                loadMoreBtn.onclick = () => window.loadPagePosts(pageId, data.paging.cursors.after);
                selector.appendChild(loadMoreBtn);
            }
        } else {
            if (!afterCursor) selector.innerHTML = '<div class="text-muted p-3 w-100 col-12 text-center">Aucune publication trouvée.</div>';
        }

    } catch (e) {
        console.error("Failed to load page posts", e);
        if (!afterCursor) selector.innerHTML = '<div class="text-danger p-3 w-100 col-12">Erreur lors du chargement des publications</div>';
    }
}

async function loadDemoPosts() {
    const selector = document.getElementById('post-grid');
    if (!selector) return;

    selector.innerHTML = '<div class="text-warning text-center p-4"><i class="bi bi-arrow-repeat spin"></i> Chargement de la démo...</div>';

    try {
        const response = await fetch('/giveaways/api/demo/posts');
        const posts = await response.json();

        selector.innerHTML = '';
        posts.filter(p => p.platform === currentPlatform).forEach(post => {
            renderPostCard(post, selector);
        });

    } catch (e) {
        console.error("Failed to load posts", e);
        selector.innerHTML = '<div class="text-danger p-3">Erreur lors du chargement de la démo</div>';
    }
}

function renderPostCard(post, container) {
    const card = document.createElement('div');
    card.className = 'post-item card text-white bg-dark mb-2 position-relative overflow-hidden cursor-pointer';
    card.dataset.id = post.id;
    card.onclick = () => {
        card.classList.toggle('selected');
        updateSelectionSummary(); // Livupdate modal footer
    };

    // Add glowing border class on selection state change handled by CSS

    const msg = post.message || post.text || "[Publication sans texte]";
    const snippet = msg.length > 80 ? msg.substring(0, 80) + "..." : msg;
    const dt = post.created_time || post.date || "";
    const dateFormatted = dt ? new Date(dt).toLocaleDateString() : "";
    const imgUrl = post.picture || 'https://placehold.co/100x100/1e2130/ffffff?text=No+Image';

    card.innerHTML = `
        <img src="${imgUrl}" alt="Post image" class="post-item-img">
        <div class="card-body py-2 px-3">
            <p class="card-text mb-1 small-text text-light">${snippet}</p>
            <p class="card-text mt-2"><small class="text-warning"><i class="bi bi-calendar-event"></i> ${dateFormatted}</small></p>
        </div>
        <div class="selection-indicator">
            <i class="bi bi-check-circle-fill fs-3"></i>
        </div>
    `;
    container.appendChild(card);
}

function openPostModal() {
    const modal = new bootstrap.Modal(document.getElementById('postsModal'));
    modal.show();
}

function updateSelectionSummary() {
    const selectedCount = document.querySelectorAll('.post-item.selected').length;

    // Update trigger button outside modal
    const summarySpan = document.getElementById('selection-summary');
    if (summarySpan) {
        summarySpan.textContent = selectedCount === 0 ? '0 publication sélectionnée' :
            `${selectedCount} publication${selectedCount > 1 ? 's' : ''} sélectionnée${selectedCount > 1 ? 's' : ''}`;
    }

    // Update footer inside modal
    const modalFooterSpan = document.getElementById('modal-selection-count');
    if (modalFooterSpan) {
        modalFooterSpan.textContent = selectedCount === 0 ? '0 sélectionnée' :
            `${selectedCount} sélectionnée${selectedCount > 1 ? 's' : ''}`;
    }
}

window.selectAllPosts = function () {
    const posts = document.querySelectorAll('.post-item');
    const allSelected = Array.from(posts).every(p => p.classList.contains('selected'));

    posts.forEach(p => {
        if (allSelected) {
            p.classList.remove('selected');
        } else {
            p.classList.add('selected');
        }
    });

    updateSelectionSummary();
}

async function startDraw() {
    if (isRunning) return;

    // Get all selected options visually
    const selectedCards = document.querySelectorAll('.post-item.selected');
    const postIds = Array.from(selectedCards).map(card => card.dataset.id);

    if (postIds.length === 0 || postIds[0] === "") {
        alert("Veuillez sélectionner au moins une publication (Cliquez sur la carte) !");
        return;
    }

    const numWinners = parseInt(document.getElementById('num_winners').value);

    const pageSelector = document.getElementById('page-selector');
    let pageToken = null;
    if (pageSelector && pageSelector.selectedIndex >= 0) {
        const selectedPageOption = pageSelector.options[pageSelector.selectedIndex];
        pageToken = selectedPageOption ? selectedPageOption.dataset.token : null;
    }

    const filters = {
        filter_duplicates: document.getElementById('filter_duplicates').checked,
        include_replies: document.getElementById('include_replies') ? document.getElementById('include_replies').checked : false,
        require_photo: document.getElementById('require_photo') ? document.getElementById('require_photo').checked : false,
        require_like: document.getElementById('require_like') ? document.getElementById('require_like').checked : false,
        date_limit: document.getElementById('filter_date_toggle') && document.getElementById('filter_date_toggle').checked ? document.getElementById('date_limit').value : null,
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
                is_live: isLiveMode,
                page_token: pageToken
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
