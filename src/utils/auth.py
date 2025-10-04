from fastapi import HTTPException, Request
import httpx
from clerk_backend_api.security.types import AuthenticateRequestOptions
from ..core.settings import DEV_MODE, DEMO_USER_ID, clerk_sdk


# Dependency to get authenticated user ID from Clerk
async def get_current_user_id(request: Request) -> str:
    if DEV_MODE:
        return DEMO_USER_ID

    # Production mode: authenticate with Clerk
    # Convert FastAPI request to httpx request for Clerk SDK
    httpx_request = httpx.Request(
        method=request.method,
        url=str(request.url),
        headers=request.headers,
    )

    try:
        request_state = clerk_sdk.authenticate_request(
            httpx_request,
            AuthenticateRequestOptions()
        )

        if not request_state.is_signed_in:
            raise HTTPException(
                status_code=401,
                detail=f"Authentication failed: {request_state.reason or 'Not signed in'}"
            )

        # Extract user_id from the token payload
        user_id = request_state.payload.get("sub")
        if not user_id:
            raise HTTPException(
                status_code=401,
                detail="User ID not found in token"
            )

        return user_id
    except Exception as e:
        if isinstance(e, HTTPException):
            raise
        raise HTTPException(status_code=401, detail=f"Authentication error: {str(e)}")

