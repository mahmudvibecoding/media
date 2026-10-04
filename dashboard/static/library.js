/* Progressive enhancements; links, search, and pagination also work without JS. */
(() => {
  let lastRecord = null;
  let noticeTimer;
  const notice = (message) => {
    const element = document.getElementById('network-notice');
    if (!element) return;
    element.textContent = message;
    element.hidden = false;
    clearTimeout(noticeTimer);
    noticeTimer = setTimeout(() => { element.hidden = true; }, 6500);
  };
  const initialize = () => {
    document.querySelectorAll('[data-fallback-image]').forEach(image => {
      const fallback = () => image.classList.add('is-broken');
      image.addEventListener('error', fallback, {once:true});
      if (image.complete && image.naturalWidth === 0) fallback();
    });
    const section = document.getElementById('workspace')?.dataset.section;
    if (section) document.title = section[0].toUpperCase()+section.slice(1)+' · Media Library';
    const drawer = document.getElementById('record-drawer');
    if (drawer && !drawer.dataset.ready) {
      drawer.dataset.ready = 'true';
      drawer.close();
      drawer.showModal();
      drawer.addEventListener('cancel', event => {
        event.preventDefault();
        drawer.querySelector('.drawer-close').click();
      });
      drawer.addEventListener('click', event => {
        if (event.target !== drawer) return;
        const rect = drawer.getBoundingClientRect();
        if (event.clientX < rect.left || event.clientX > rect.right) drawer.querySelector('.drawer-close').click();
      });
    }
  };
  document.addEventListener('click', event => {
    const link = event.target.closest('.record-link');
    if (link) lastRecord = link;
  });
  document.addEventListener('change', event => {
    if (event.target.matches('[data-submit-on-change]')) event.target.form.requestSubmit();
  });
  document.addEventListener('keydown', event => {
    if (event.key !== '/' || event.ctrlKey || event.metaKey || event.altKey || event.target.closest('input,textarea,select,[contenteditable]') || document.querySelector('dialog[open]')) return;
    const input = document.querySelector('.search-input');
    if (input) { event.preventDefault(); input.focus(); input.select(); }
  });
  document.addEventListener('htmx:beforeSwap', event => {
    const status = event.detail.xhr.status;
    if ([400,404,503].includes(status)) {
      if (event.detail.target?.id === 'detail-layer') {
        notice(status === 503 ? 'The library is temporarily unavailable. Please try again.' : 'This record or page could not be opened. Refresh and try again.');
      } else {
        event.detail.shouldSwap = true;
        event.detail.isError = false;
      }
    }
  });
  document.addEventListener('htmx:afterSwap', event => {
    initialize();
    if (!document.getElementById('record-drawer') && lastRecord?.isConnected) lastRecord.focus({preventScroll:true});
    if (event.detail.target?.id === 'workspace') window.scrollTo({top:0,behavior:'instant'});
  });
  document.addEventListener('htmx:historyRestore', initialize);
  document.addEventListener('htmx:sendError', () => notice('Connection interrupted. Please try again.'));
  document.addEventListener('htmx:timeout', () => notice('The request took too long. Please try again.'));
  document.addEventListener('DOMContentLoaded', initialize);
})();
