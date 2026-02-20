from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse
from sqlalchemy.ext.asyncio import AsyncSession
from app.db import get_db
from app.deps import web_require_permission
from fastapi.templating import Jinja2Templates

router = APIRouter(
    prefix="/giveaways",
    tags=["giveaways"],
)

templates = Jinja2Templates(directory="app/frontend/templates")

@router.get("/", response_class=HTMLResponse, name="giveaways_page")
async def giveaways_page(
    request: Request,
    db: AsyncSession = Depends(get_db),
    # Optional permission: Require user to just be logged in, or we can use admin.
    # For now, require login:
    user: dict = Depends(web_require_permission("is_admin")) # Change if you want non-admins to use it
):
    """
    Main Giveaway Tool Page.
    """
    context = {
        "request": request,
        "user": user,
        "app_name": "Bijouterie Zaher"
    }
    return templates.TemplateResponse("giveaway.html", context)

# --- DEMO API ENDPOINTS ---

@router.get("/api/demo/posts")
async def get_demo_posts():
    """Return fake posts for the demo UI."""
    return [
        {"id": "post_1", "text": "Win a 24k Gold Necklace! Tag 2 friends.", "date": "2026-02-15", "platform": "facebook"},
        {"id": "post_2", "text": "Diamond Ring Giveaway 💍 Comment your favorite emoji!", "date": "2026-02-10", "platform": "instagram"}
    ]

@router.post("/api/draw")
async def draw_winners(
    request: Request,
    db: AsyncSession = Depends(get_db),
    user: dict = Depends(web_require_permission("is_admin"))
):
    """
    The core endpoint to pick a winner.
    In Live Mode: Queries Facebook Graph API.
    In Demo Mode: Uses a simulated pool of comments.
    """
    data = await request.json()
    post_id = data.get("post_id")
    platform = data.get("platform", "facebook")
    num_winners = int(data.get("num_winners", 1))
    
    # filters = data.get("filters", {})
    # e.g., filter_duplicates, filter_mentions

    from app.services.giveaway import GiveawayService
    
    # We will build this service next.
    winners = await GiveawayService.draw_winners(
        db=db,
        post_id=post_id,
        platform=platform,
        num_winners=num_winners,
        filters=data.get("filters", {})
    )
    
    return {"status": "success", "winners": winners}
