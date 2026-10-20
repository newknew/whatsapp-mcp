# whatsapp-mcp

MCP server for WhatsApp messaging via [whatsmeow](https://github.com/tulir/whatsmeow).

## Features

- Send and receive messages (text, images, documents)
- List chats and contacts
- Search messages
- Download attachments
- Read receipts and dismiss tracking
- Group info, member lists, invite link lookup
- Find groups by name or member

## Groups

- Names and members sync on connect and on group events
- `whatsapp_list_chats` takes `query` and `participant`
- `whatsapp_get_group_info` takes `chat_jid` or `invite_link`
- Live lookups are rate-limited by WhatsApp; cache fills in

## Prerequisites

- Go 1.24+
- A WhatsApp account (phone number for QR pairing)

## Install

```bash
go install github.com/newknew/whatsapp-mcp@main
```

## First run

On first launch, the server displays a QR code in the
terminal. Scan it with WhatsApp on your phone to pair.

```bash
whatsapp-mcp
```

The session is stored in
`~/.local/share/newknew-mcp/whatsapp/` and persists across
restarts.

## Usage

Runs as an HTTP server on `localhost:2053`. Health check:

```bash
curl http://localhost:2053/health
```

## Data paths

- Session DB: `~/.local/share/newknew-mcp/whatsapp/`
- Debug log: `~/.local/log/newknew-mcp/whatsapp.log`
- Downloaded files: `/tmp/newknew_whatsapp_*`

## License

MIT
