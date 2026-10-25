# kojo-ext-square

[日本語](README.ja.md)

An extension package that wakes a [kojo](https://github.com/loppo-llc/kojo) agent when something happens in a Square seller account.

- **New orders**, orders that were **completed** or **canceled**
- **Completed** and **failed / canceled** payments
- **Refunds** (new ones, and status changes)
- Everything found in one poll reaches the agent **together, as one turn**; while the agent is still working on it, the next batch goes **into the running turn**
- Filters by event type, minimum amount, and location

It only **polls** Square's APIs: there is no webhook and no public URL to expose. It never calls a model itself either. The message the agent gets is a short list ("order X, 1,200 JPY, completed, at Shibuya"); the agent looks up the details (line items, customer, tenders) with the extension's own **read-only MCP server** if it needs them.

## Requirements

- A kojo build **newer than v0.135.0** (needs the `chat:send` scope and extension secret settings; builds after v0.139.0 label the message as a system notice instead of a user message)
- A Square seller account and an access token for it
- Nothing else: the lookup tools come with the extension (see [Lookup tools](#lookup-tools)). Square's own MCP server is not needed

## 1. Square setup

1. Open the [Square Developer Console](https://developer.squareup.com/apps) and create an application (or open an existing one)
2. On **Credentials**, pick **Production** (or **Sandbox** to try it on test data) and copy the **access token**

A personal access token from the console has full access to its account. If you use an OAuth token instead, it needs:

| Permission | Used for |
| --- | --- |
| `ORDERS_READ` | SearchOrders |
| `PAYMENTS_READ` | ListPayments, ListPaymentRefunds |
| `MERCHANT_PROFILE_READ` | ListLocations (location names, and the location list when none are configured) |
| `CUSTOMERS_READ` | Optional: the `get_customer` / `search_customers` lookup tools |
| `ITEMS_READ` | Optional: the `search_catalog_items` lookup tool |

> The token can read your sales. Do not paste it into chats or repositories. If it leaks, replace it in the Developer Console.

Production and sandbox tokens only work against their own environment, so set **Environment** to match the token.

To restrict the extension to some locations you need their IDs: **Locations** in the Developer Console, or the Square Dashboard under **Account & Settings → Locations**.

## 2. kojo setup

1. Open **Settings → Extensions** in kojo
2. Enter `https://github.com/loppo-llc/kojo-ext-square` as the **repository URL** and click the button that inspects it
3. Review the requested scope (`chat:send` — send messages to the agent) and approve the install
4. Open **Square** in the installed list and enable it for the agent that should be woken
5. Paste the access token into **Access token** in that agent's settings, pick the **Environment**, and save

Saving starts the service for that agent. The first poll records "now" as its starting point; nothing that happened before the install is reported.

The token is stored encrypted in kojo's credential store and never appears again in the UI or the API.

**Which agent is woken**: the agent the extension is enabled for. Each enabled agent has its own poller, its own position and its own settings, so two agents can watch two different Square accounts (or the same account with different filters).

### Settings

| Setting | Default | Description |
| --- | --- | --- |
| Access token | — | Required. A Square access token |
| Environment | production | `production` (connect.squareup.com) or `sandbox` (connect.squareupsandbox.com) |
| Location IDs | empty | Locations to watch, comma-separated. Empty means every location of the account that is not inactive. A list holding nothing that reads as an ID watches nothing (and the agent is warned) |
| Poll interval (seconds) | 30 | How often to ask Square for changes (10–3600) |
| Minimum amount | 0 | Report only events of at least this amount, in the currency's main unit (`1000` = ¥1,000 or $1,000). Compared per event in that event's own currency. Warnings are always reported |
| New orders | on | `order.created` |
| Orders completed | on | `order.state_changed` to `COMPLETED` |
| Orders canceled | on | `order.state_changed` to `CANCELED` |
| Payments completed | on | `payment.completed` |
| Payments failed or canceled | on | `payment.failed` |
| Refunds | on | `refund` |

## What the agent receives

One message per poll that found something, posted into the agent's main chat, so the notice and the agent's reply show up in the kojo UI:

```
[Square プラグイン (kojo-ext-square) からの自動通知]
人ではなく拡張が自動で送っている。あなたの返答はこのチャットに表示され、ユーザーが読む。
Square で 3 件の動きがありました。詳細 (明細・顧客・決済内容など) は square MCP の get_order / get_payment などで確認してください。

- order.created | id=ORDER1 | 1,200 JPY | state=OPEN | items=コーヒー×2 | location=渋谷店 (L1)
- payment.completed | id=PAY1 | 1,200 JPY | state=COMPLETED | order=ORDER1 | location=渋谷店 (L1)
- refund | id=REF1 | 200 JPY | state=PENDING | payment=PAY0 | location=渋谷店 (L1)
```

The message text is Japanese; the system prompt addition that comes with it tells the agent (in English) that this turn was started by the extension, that the details are behind the extension's lookup tools, and that its reply is not delivered anywhere — whatever it wants to do (tell someone in Slack, write a note, reply to a customer) it does with its own tools.

| Event | When |
| --- | --- |
| `order.created` | An order created since the last poll, or an order taken out of `DRAFT`. Drafts themselves are not reported |
| `order.state_changed` | A known order changed state (`state=OPEN→COMPLETED`), or an order first seen already `COMPLETED` / `CANCELED` that was closed after the install |
| `payment.completed` | A payment reached `COMPLETED` (including a card authorization from before the install that is captured after it) |
| `payment.failed` | A payment ended `FAILED` or `CANCELED` (including a void after the install) |
| `refund` | A new refund, a refund's status changed, or a refund from before the install that settled after it. If a payment's refunded amount goes up and no refund record for it has been seen, that is reported as a refund too, and the refund record that shows up later is not reported again as new |
| `plugin.warning` / `plugin.recovered` | The extension's own trouble (below) |

Orders that record a return are labelled. Refunding a payment makes Square create a return-only order, shown as `order.created (return)` with `returned=` (the returned total), `returned_items=`, and `return_of=` (the original order, when Square links one). An order with both sold and returned items is `(exchange)`: the usual amount and `items=` are the sold side, `returned=` / `returned_items=` the returned side. The minimum-amount filter lets an event through if either side reaches it.

Up to 50 lines are listed; the rest are counted ("ほか N 件").

## Lookup tools

The extension also gives each agent it is enabled for an MCP server named `square` (in Claude Code the tools appear as `mcp__square__…`). It uses the same access token and location setting as the poller, and it **only reads**: nothing in it creates, changes or cancels anything in Square.

| Tool | What it returns |
| --- | --- |
| `list_locations` | The locations the agent may look at |
| `search_orders` | Orders, newest first, as compact rows; filter by location, state, creation time, customer |
| `get_order` | One order as Square returns it (line items, modifiers, notes, taxes, discounts, fulfillments, tenders, returns) |
| `list_payments` | Payments at one location, newest first, as compact rows |
| `get_payment` | One payment in full (card, fees, refunded amount, receipt) |
| `list_refunds` / `get_refund` | Refunds, as compact rows / in full |
| `get_customer` / `search_customers` | Customer profiles; search by partial email or phone (`CUSTOMERS_READ`) |
| `search_catalog_items` | Catalog items by keyword, with variations and prices (`ITEMS_READ`) |

When locations are configured, orders, payments, refunds and catalog items stay inside them: one at another location (or with no location) is refused, and list results are filtered. Customers are the exception: Square keeps them per account, not per location, so the customer tools see every customer the token can. The location setting narrows what the agent is shown; it is not an access boundary, since the token itself reaches the whole account. Leave out `CUSTOMERS_READ` if the agent should not see customers. Lists page with `limit` / `cursor`. The access token is fetched from kojo when the first tool runs, so it never appears in the MCP server's environment or command line.

## How it works

- **Polling**: every interval it calls SearchOrders (up to 10 locations per request), ListPayments (one location per request), and ListPaymentRefunds, each filtered on `updated_at` and sorted oldest first, following the pagination cursor. Every request sends `Square-Version: 2026-09-16`
- **Position**: each stream (orders, payments, refunds) keeps a cursor, the newest `updated_at` seen. The next poll starts **2 minutes before** the cursor, to catch rows Square's eventually consistent search shows late. What comes back twice is recognised by a remembered fingerprint (version / status) per object and not reported again. The remembered set is bounded (5,000 objects per stream, oldest dropped first). A poll that hits the page limit (40 pages per query) resumes from where it stopped, without the overlap, until it has caught up
- **No loss on restart**: the position, the remembered set, and the not-yet-delivered events are saved together in `state.json` in the extension's data directory (written to a temp file, synced, and renamed). The position only moves forward in the same write that queues the events it found. A stream whose poll failed keeps its position and is retried
- **Delivery**: the queued events become one message. It is posted to the agent's main chat, which kojo accepts only while the agent is idle; a running turn answers busy and the message is retried every 15 seconds. If kojo is down or answers with another error, the events stay queued and are retried with backoff. Events leave the queue only once kojo has accepted them (so a crash at the wrong moment can repeat a notification, but never lose one). The queue is bounded at 1,000 events; if it ever overflows, the oldest are dropped and the next message says how many
- **Rate limits and errors**: 429 and 5xx answers are retried with exponential backoff and jitter (honouring `Retry-After`). 401 / 403 are not retried
- **Warnings**: when Square rejects the token (401) the agent is told right away; after 5 failed polls in a row for any other reason it is told once. When polling works again it is told once that things recovered. While the token is rejected, polling slows to every 5 minutes

## Troubleshooting

| Symptom | Check |
| --- | --- |
| Nothing ever arrives | Was the token saved, for the right **Environment**? Are both the extension and the agent enabled? Only things that happen **after** the install are reported. Are the filters (minimum amount, location IDs) too strict? |
| `plugin.warning` mentioning 401 | The token is wrong, revoked, or for the other environment. Save a new one |
| `plugin.warning` mentioning 403 | The OAuth token lacks one of the permissions in section 1 |
| Location names missing | `MERCHANT_PROFILE_READ` is missing; IDs are shown instead |
| `no Square access token configured` in the log | No token saved yet; the service waits |

Logs go to kojo's own log, tagged `extension=square`.

## Limitations

- **No backfill**: the first poll starts at the time of the install (or after the saved state was lost)
- **Offline POS sales** can reach Square up to 72 hours late. One that arrives after its order has fallen out of the 2-minute window behind the cursor is not reported
- Square's search is eventually consistent; a change can show up a poll later than it happened
- No webhooks: the delay is up to one poll interval plus Square's own lag
- A very busy account (more than 40 pages of changes per stream per poll) is caught up over several polls
- Payments and refunds created more than a year ago are not looked at (Square's default `begin_time` for these lists). Square does not allow refunds that late, so in practice nothing reportable is lost

## Development

```sh
make test    # go vet + go test
make build   # build binaries for every platform under bin/
```

kojo installs an extension package by `git clone` and does not build it, so the binaries are committed to the repository. After changing code, run `make build`, commit `bin/` along with it, and bump `version` in `kojo-package.json`.

The tests use `httptest` stand-ins for Square and kojo; they make no real Square calls.

## License

MIT
