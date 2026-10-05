"""Media Library: searchable pages with optional authentication."""
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime
import hashlib
import hmac
import logging
import os
from pathlib import Path
import re
import secrets
from urllib.parse import quote, urlsplit

from fastapi import APIRouter, Depends, FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from markupsafe import Markup, escape
import psycopg
from psycopg.conninfo import make_conninfo
from starlette.middleware.sessions import SessionMiddleware

from dashboard.data import DEFAULT_SORTS, PAGE_SIZE, SORTS, Repository, Selection
from dashboard.search import normalize, terms
from dashboard.security import verify_password
from runtime_config import database_options

ROOT = Path(__file__).parent
logger = logging.getLogger('media-library')


@dataclass
class Settings:
    username: str = 'mahmud'
    password_hash: str = ''
    secret: str = ''
    secure_cookies: bool = False
    conninfo: str = ''
    auth_required: bool = True

    @classmethod
    def environment(cls):
        return cls(username=os.environ.get('MEDIA_DASHBOARD_USERNAME','mahmud'),
            password_hash=os.environ.get('MEDIA_DASHBOARD_PASSWORD_HASH',''),
            secret=os.environ.get('MEDIA_DASHBOARD_SECRET',''),
            secure_cookies=os.environ.get('MEDIA_DASHBOARD_SECURE_COOKIES') == '1',
            auth_required=os.environ.get('MEDIA_DASHBOARD_AUTH_REQUIRED') != '0',
            conninfo=make_conninfo(**database_options('media')))

    @property
    def version(self):
        return hashlib.sha256(self.password_hash.encode()).hexdigest()[:20]


def safe_url(value):
    try:
        url = urlsplit(value or '')
        if url.scheme in ('http','https') and url.hostname and not url.username and not url.password:
            return value
    except ValueError:
        pass
    return ''


def image_url(value):
    url = safe_url(value)
    return url if url.startswith('https://') else ''


def highlighted(value, query='', limit=None):
    text = str(value or '')
    needles = terms(query)
    pattern = re.compile('|'.join(re.escape(item) for item in needles),re.I) if needles else None
    normalized = normalize(text)
    match = pattern.search(normalized) if pattern else None
    start = max(0,match.start()-60) if limit and match and match.start() >= limit else 0
    if limit and len(text) > limit:
        text = ('…' if start else '')+text[start:start+limit]+('…' if start+limit < len(text) else '')
        normalized = normalize(text)
    if not pattern:
        return escape(text)
    pieces, position = [], 0
    for match in pattern.finditer(normalized):
        pieces.extend((escape(text[position:match.start()]), Markup('<mark>'), escape(text[match.start():match.end()]), Markup('</mark>')))
        position = match.end()
    pieces.append(escape(text[position:]))
    return Markup('').join(pieces)


def number(value):
    return f'{value:,}' if value is not None else '—'


def compact(value):
    if value is None:
        return '—'
    for size, suffix in ((1000000000,'B'),(1000000,'M'),(1000,'K')):
        if abs(value) >= size:
            return f'{value/size:.2f}'.rstrip('0').rstrip('.')+suffix
    return str(value)


def date_label(value):
    if not value:
        return '—'
    return value.strftime('%b %d, %Y').replace(' 0',' ')


def duration(value):
    if value is None:
        return '—'
    hours, remainder = divmod(value,3600)
    minutes, seconds = divmod(remainder,60)
    return f'{hours}:{minutes:02}:{seconds:02}' if hours else f'{minutes}:{seconds:02}'


def initial(value):
    return next((letter.upper() for letter in str(value or '') if letter.isalnum()), '?')


def next_path(value):
    if value and value.startswith(('/channels','/videos','/comments')) and '\n' not in value and '\r' not in value:
        return value
    return '/channels'


def create_app(settings=None, repository=None):
    settings = settings or Settings.environment()
    repo = repository or Repository(settings.conninfo, settings.secret)

    @asynccontextmanager
    async def lifespan(app):
        if len(settings.secret) < 32 or (settings.auth_required and not settings.password_hash.startswith('pbkdf2_sha256$')):
            raise RuntimeError('Configure dashboard access with sh scripts/start-dashboard.sh')
        repo.open()
        try:
            yield
        finally:
            repo.close()

    app = FastAPI(lifespan=lifespan,docs_url=None,redoc_url=None,openapi_url=None)
    app.state.repository = repo
    app.add_middleware(SessionMiddleware,secret_key=settings.secret or 'unconfigured',session_cookie='media_library',
                       same_site='lax',https_only=settings.secure_cookies,max_age=7*24*3600)
    templates = Jinja2Templates(directory=ROOT/'templates')
    templates.env.filters.update(number=number,compact=compact,date_label=date_label,duration=duration,
                                 safe_url=safe_url,image_url=image_url,highlight=highlighted,initial=initial)
    templates.env.globals.update(SORTS=SORTS,PAGE_SIZE=PAGE_SIZE)

    @app.middleware('http')
    async def headers(request, call_next):
        response = await call_next(request)
        response.headers.update({
            'Cache-Control':'private, no-store',
            'X-Content-Type-Options':'nosniff',
            'X-Frame-Options':'DENY',
            'Referrer-Policy':'no-referrer',
            'Content-Security-Policy':"default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' https: data:; connect-src 'self'; object-src 'none'; base-uri 'self'; frame-ancestors 'none'; form-action 'self'",
        })
        return response

    def authenticated(request):
        return not settings.auth_required or (request.session.get('user') == settings.username
                                             and request.session.get('version') == settings.version)

    def require_login(request: Request):
        if not authenticated(request):
            destination = '/login?next='+quote(next_path(request.url.path+('?' + request.url.query if request.url.query else '')),safe='')
            if request.headers.get('HX-Request') == 'true':
                raise HTTPException(401,headers={'HX-Redirect':destination})
            raise HTTPException(303,headers={'Location':destination})

    def csrf(request, value):
        expected = request.session.get('csrf','')
        if not expected or not hmac.compare_digest(str(value),expected):
            raise HTTPException(403,'Please reload the page and try again.')

    def render(request, template, context, status=200):
        return templates.TemplateResponse(request=request,name=template,context={
            'username':settings.username,'csrf':request.session.get('csrf',''),
            'auth_required':settings.auth_required,**context},status_code=status)

    app.mount('/assets',StaticFiles(directory=ROOT/'static'),name='assets')

    @app.get('/health')
    def health():
        with repo.pool.connection() as conn:
            conn.execute('SELECT 1')
        return {'ready':True}

    @app.get('/')
    def index():
        return RedirectResponse('/channels',status_code=303)

    @app.get('/login',response_class=HTMLResponse)
    def login_page(request: Request, next: str=''):
        if authenticated(request):
            return RedirectResponse(next_path(next),status_code=303)
        request.session['csrf'] = secrets.token_urlsafe(24)
        return render(request,'login.html',dict(next=next_path(next),error=''))

    @app.post('/login')
    async def login(request: Request):
        if not settings.auth_required:
            return RedirectResponse('/channels',status_code=303)
        form = await request.form(max_fields=5,max_files=0)
        csrf(request,form.get('csrf',''))
        password = str(form.get('password',''))
        user = str(form.get('username',''))
        valid = len(password) <= 256 and verify_password(password,settings.password_hash)
        if not valid or not hmac.compare_digest(user.encode(),settings.username.encode()):
            return render(request,'login.html',dict(next=next_path(str(form.get('next',''))),error='That username or password is incorrect.'),401)
        request.session.clear()
        request.session.update(user=settings.username,version=settings.version,csrf=secrets.token_urlsafe(24))
        return RedirectResponse(next_path(str(form.get('next',''))),status_code=303)

    router = APIRouter(dependencies=[Depends(require_login)])

    @router.post('/logout')
    async def logout(request: Request):
        if not settings.auth_required:
            return RedirectResponse('/channels',status_code=303)
        form = await request.form(max_fields=2,max_files=0)
        csrf(request,form.get('csrf',''))
        request.session.clear()
        return RedirectResponse('/login',status_code=303)

    @router.get('/{kind}',response_class=HTMLResponse)
    def library(request: Request, kind: str):
        if kind not in SORTS:
            raise HTTPException(404)
        partial = request.headers.get('HX-Request') == 'true' and request.headers.get('HX-History-Restore-Request') != 'true'
        drawer = partial and request.headers.get('HX-Target') == 'detail-layer'
        try:
            selected = Selection.parse(kind,request.query_params)
            detail = repo.detail(selected) if selected.detail else None
            if drawer:
                return render(request,'drawer.html',dict(selected=selected,detail=detail))
            context = repo.context(selected)
            result = repo.search(selected)
            totals = repo.totals()
            return render(request,'workspace.html' if partial else 'library.html',
                dict(selected=selected,result=result,totals=totals,detail=detail,scope=context,error=''))
        except (ValueError,LookupError) as exc:
            status = 404 if isinstance(exc,LookupError) else 400
            selected = Selection(kind=kind,sort=DEFAULT_SORTS[kind])
            message = str(exc)
        except psycopg.errors.QueryCanceled:
            status, message = 200, 'This search is taking longer than expected. Try a more specific phrase or select a channel or video.'
            selected = Selection.parse(kind,request.query_params)
        except (psycopg.Error,TimeoutError):
            logger.exception('Library query failed')
            status, message = 503, 'The library is temporarily unavailable. Please try again in a moment.'
            selected = Selection(kind=kind,sort=DEFAULT_SORTS[kind])
        if drawer:
            return HTMLResponse(str(escape(message)),status_code=status if status >= 400 else 503)
        result = dict(rows=[],page=1,previous=None,next=None,seconds=0,sort_label='')
        totals = repo._totals or dict(channels=None,videos=None,comments=None,updated_at=None)
        return render(request,'workspace.html' if partial else 'library.html',
            dict(selected=selected,result=result,totals=totals,detail=None,scope={},error=message),status)

    app.include_router(router)
    return app


app = create_app()
