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

import os
import httpx
from urllib.parse import urlencode
from fastapi.responses import RedirectResponse

# --- LIVE OAUTH API ENDPOINTS ---

@router.get("/auth/login")
async def facebook_login(request: Request):
    """Refers the user to the Meta OAuth login page."""
    app_id = os.getenv("FB_APP_ID")
    if not app_id:
        # Prevent crash if user clicks it without setting up .env yet
        return RedirectResponse(url="/giveaways/?error=missing_fb_keys")
        
    redirect_uri = "https://hr-sync.onrender.com/giveaways/auth/callback"
    
    # We request permissions to read pages and manage comments
    permissions = "pages_show_list,pages_read_engagement,pages_manage_metadata,business_management"
    
    oauth_url = f"https://www.facebook.com/v19.0/dialog/oauth?" + urlencode({
        "client_id": app_id,
        "redirect_uri": redirect_uri,
        "scope": permissions + ",instagram_basic,instagram_manage_comments",
        "response_type": "code"
    })
    
    return RedirectResponse(url=oauth_url)

@router.get("/auth/logout")
async def facebook_logout(request: Request):
    """Disconnect Meta account by clearing token from session and cookies."""
    if "fb_access_token" in request.session:
        del request.session["fb_access_token"]
    response = RedirectResponse(url="/giveaways/")
    response.delete_cookie("fb_token")
    return response

@router.get("/auth/callback", name="facebook_callback")
async def facebook_callback(request: Request, code: str = None, error: str = None):
    """Handles the OAuth redirect from Meta."""
    if error or not code:
        return RedirectResponse(url="/giveaways/?error=auth_failed")
        
    app_id = os.getenv("FB_APP_ID")
    app_secret = os.getenv("FB_APP_SECRET")
    redirect_uri = str(request.url_for("facebook_callback"))
    
    # Exchange code for token
    token_url = "https://graph.facebook.com/v19.0/oauth/access_token"
    async with httpx.AsyncClient() as client:
        resp = await client.get(token_url, params={
            "client_id": app_id,
            "redirect_uri": redirect_uri,
            "client_secret": app_secret,
            "code": code
        })
        
        data = resp.json()
        if "access_token" in data:
            # Store safely in session
            request.session["fb_access_token"] = data["access_token"]
            response = RedirectResponse(url="/giveaways/?success=connected")
            response.set_cookie(
                key="fb_token", 
                value=data["access_token"], 
                httponly=True, 
                secure=True, 
                samesite='lax',
                max_age=3600*24*60
            )
            return response
            
    return RedirectResponse(url="/giveaways/?error=token_exchange_failed")

@router.get("/api/live/pages")
async def get_live_pages(request: Request, platform: str = "facebook"):
    """Fetches Facebook Pages or Linked Instagram Accounts the user manages."""
    token = request.cookies.get("fb_token") or request.session.get("fb_access_token") or os.getenv("FB_ACCESS_TOKEN")
    if not token:
        return {"error": "not_authenticated"}
        
    async with httpx.AsyncClient() as client:
        # Request both FB pages and their linked IG accounts
        resp = await client.get("https://graph.facebook.com/v19.0/me/accounts", params={
            "access_token": token,
            "fields": "id,name,access_token,instagram_business_account{id,username,profile_picture_url}"
        })
        
        data = resp.json()
        
        # If it fails (e.g. user hasn't explicitly granted Instagram permissions on a previous login)
        if "error" in data:
            fallback_resp = await client.get("https://graph.facebook.com/v19.0/me/accounts", params={
                "access_token": token,
                "fields": "id,name,access_token"
            })
            fallback_data = fallback_resp.json()
            if "error" not in fallback_data:
                data = fallback_data
            else:
                return data # If both fail, return the original error
            
        result = []
        for page in data.get("data", []):
            if platform == "instagram":
                ig_account = page.get("instagram_business_account")
                if ig_account:
                    result.append({
                        "id": ig_account["id"],
                        "name": f"{ig_account.get('username', 'Instagram')} (via {page['name']})",
                        "access_token": page["access_token"],
                        "platform": "instagram",
                        "page_id": page["id"]
                    })
            else:
                result.append({
                    "id": page["id"],
                    "name": page["name"],
                    "access_token": page["access_token"],
                    "platform": "facebook"
                })
                
        return {"data": result}

@router.get("/api/live/posts/{page_id}")
async def get_live_posts(request: Request, page_id: str, platform: str = "facebook", page_token: str = None):
    """Fetches Posts for a specific Page or Instagram Account."""
    token = page_token or request.cookies.get("fb_token") or request.session.get("fb_access_token") or os.getenv("FB_ACCESS_TOKEN")
    if not token:
        return {"error": "not_authenticated"}
        
    async with httpx.AsyncClient() as client:
        if platform == "instagram":
            resp = await client.get(f"https://graph.facebook.com/v19.0/{page_id}/media", params={
                "access_token": token,
                "fields": "id,caption,timestamp",
                "limit": 20
            })
            data = resp.json()
            if "error" in data:
                return data
                
            posts = []
            for media in data.get("data", []):
                posts.append({
                    "id": media["id"],
                    "message": media.get("caption", "[No Caption]"),
                    "created_time": media.get("timestamp", "")
                })
            return {"data": posts}
        else:
            resp = await client.get(f"https://graph.facebook.com/v19.0/{page_id}/posts", params={
                "access_token": token,
                "fields": "id,message,created_time",
                "limit": 20
            })
            return resp.json()

@router.get("/api/live/debug")
async def debug_live_fb(request: Request):
    """Endpoint for debugging Meta token issues."""
    token = request.cookies.get("fb_token") or request.session.get("fb_access_token")
    if not token:
        return {"status": "error", "message": "No token found in cookies or session."}
        
    async with httpx.AsyncClient() as client:
        # 1. Who am I?
        me_resp = await client.get("https://graph.facebook.com/v19.0/me", params={"access_token": token})
        
        # 2. What permissions do I have?
        perm_resp = await client.get("https://graph.facebook.com/v19.0/me/permissions", params={"access_token": token})
        
        # 3. What accounts do I have?
        acc_resp = await client.get("https://graph.facebook.com/v19.0/me/accounts", params={"access_token": token})
        
        return {
            "token_starts_with": token[:10] + "...",
            "me": me_resp.json(),
            "permissions": perm_resp.json(),
            "accounts": acc_resp.json()
        }


# --- DEMO API ENDPOINTS ---

@router.get("/api/demo/posts")
async def demo_posts():
    """Returns placeholder demo posts."""
    return [
        {"id": "demo_1", "text": "Post promotionnel de test", "date": "2024-05-10", "platform": "facebook"},
        {"id": "demo_2", "text": "Concours Saint Valentin", "date": "2024-02-14", "platform": "facebook"}
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
    
    # Handle both single and array of post IDs
    post_ids = data.get("post_ids", [])
    if "post_id" in data and not post_ids:
        post_ids = [data["post_id"]]
        
    platform = data.get("platform", "facebook")
    num_winners = int(data.get("num_winners", 1))
    
    # Grab the token from cookies
    page_token = data.get("page_token")
    fb_token = page_token or request.cookies.get("fb_token") or request.session.get("fb_access_token")

    from app.services.giveaway import GiveawayService
    
    # We will build this service next.
    winners = await GiveawayService.draw_winners(
        db=db,
        post_ids=post_ids,
        platform=platform,
        num_winners=num_winners,
        filters=data.get("filters", {}),
        fb_token=fb_token
    )
    
    return {"status": "success", "winners": winners}
