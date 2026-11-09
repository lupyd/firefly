// Test-only stand-in for lupyd-payments, the subscription service firefly-mls asks before
// creating groups or changing members. Every user gets the same unlimited plan that
// firefly-mls's own emulator fallback returns. That fallback is only compiled into server
// builds with the test-hooks feature; the published server image doesn't have it.
//
//   bun examples/bots-e2e/fake-subscriptions.ts
//
// Start the server with SUBSCRIPTIONS_URL=http://127.0.0.1:8088 and any non-empty
// SUBSCRIPTIONS_AUTHORIZATION (it is not checked here).
const PORT = Number(process.env.FAKE_SUBSCRIPTIONS_PORT || 8088);
const CONSTRAINTS = {
  post_file_size_mb: 1000,
  message_file_size_mb: 1000,
  max_groups: 1000,
  max_group_members: 1000,
  max_channels: 64,
  max_roles: 64,
  group_file_size_mb: 1000,
  has_ads: false,
};
Bun.serve({
  port: PORT,
  hostname: '0.0.0.0',
  fetch(req) {
    const path = new URL(req.url).pathname;
    if (/^\/lupyd\/plans\/[^/]+\/constraints$/.test(path)) {
      console.log(`plan constraints: ${path}`);
      return Response.json(CONSTRAINTS);
    }
    const m = path.match(/^\/lupyd\/users\/([^/]+)\/subscription$/);
    if (!m) return new Response('not found', { status: 404 });
    console.log(`subscription lookup: ${decodeURIComponent(m[1])}`);
    return Response.json({
      username: decodeURIComponent(m[1]),
      app_id: 1,
      plan_id: 3,
      subscription_id: 1,
      billing_cycle: 0,
      status: 2,
      current_period_start: 0,
      current_period_end: 9007199254740991,
      cancel_requested: false,
      constraints: CONSTRAINTS,
    });
  },
});
console.log(`fake subscriptions listening on ${PORT}`);
