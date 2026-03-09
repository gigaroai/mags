defmodule CortexWeb.Layouts do
  use Phoenix.Component

  def root(assigns) do
    ~H"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
      <meta charset="utf-8" />
      <meta name="viewport" content="width=device-width, initial-scale=1" />
      <title>Cortex — MAGS Nerve Center</title>
      <style>
        @font-face { font-family: 'JetBrains Mono'; font-weight: 300; src: url('/cortex/assets/fonts/JetBrainsMono-Light.woff2') format('woff2'); }
        @font-face { font-family: 'JetBrains Mono'; font-weight: 400; src: url('/cortex/assets/fonts/JetBrainsMono-Regular.woff2') format('woff2'); }
        @font-face { font-family: 'JetBrains Mono'; font-weight: 500; src: url('/cortex/assets/fonts/JetBrainsMono-Medium.woff2') format('woff2'); }
        @font-face { font-family: 'JetBrains Mono'; font-weight: 700; src: url('/cortex/assets/fonts/JetBrainsMono-Bold.woff2') format('woff2'); }
        @font-face { font-family: 'Outfit'; font-weight: 300; src: url('/cortex/assets/fonts/Outfit-Light.woff2') format('woff2'); }
        @font-face { font-family: 'Outfit'; font-weight: 400; src: url('/cortex/assets/fonts/Outfit-Regular.woff2') format('woff2'); }
        @font-face { font-family: 'Outfit'; font-weight: 600; src: url('/cortex/assets/fonts/Outfit-SemiBold.woff2') format('woff2'); }
        @font-face { font-family: 'Outfit'; font-weight: 700; src: url('/cortex/assets/fonts/Outfit-Bold.woff2') format('woff2'); }

        :root {
          --bg: #0a0a0f;
          --bg-card: #111118;
          --bg-card-hover: #16161f;
          --border: #1e1e2e;
          --text: #e0e0e8;
          --text-dim: #6b6b80;
          --accent: #6ee7b7;
          --accent-dim: #2d4a3e;
          --agent: #818cf8;
          --service: #f59e0b;
          --idle: #6ee7b7;
          --busy: #f59e0b;
          --error: #ef4444;
          --offline: #4b5563;
        }

        * { margin: 0; padding: 0; box-sizing: border-box; }

        body {
          font-family: 'Outfit', sans-serif;
          background: var(--bg);
          color: var(--text);
          min-height: 100vh;
        }

        .mono { font-family: 'JetBrains Mono', monospace; }

        .container {
          max-width: 1200px;
          margin: 0 auto;
          padding: 2rem;
        }

        header {
          display: flex;
          align-items: center;
          justify-content: space-between;
          margin-bottom: 2rem;
          padding-bottom: 1rem;
          border-bottom: 1px solid var(--border);
        }

        header h1 {
          font-size: 1.5rem;
          font-weight: 700;
          letter-spacing: -0.02em;
        }

        header h1 span { color: var(--accent); }

        .status-badge {
          font-family: 'JetBrains Mono', monospace;
          font-size: 0.75rem;
          padding: 0.25rem 0.75rem;
          border-radius: 9999px;
          background: rgba(239,68,68,0.15);
          color: var(--error);
          border: 1px solid var(--error);
          transition: all 0.3s ease;
        }

        .status-badge.connected {
          background: var(--accent-dim);
          color: var(--accent);
          border: 1px solid var(--accent);
          animation: pulse 2s ease-in-out infinite;
        }

        @keyframes pulse {
          0%, 100% { opacity: 1; box-shadow: 0 0 4px var(--accent-dim); }
          50% { opacity: 0.6; box-shadow: 0 0 12px var(--accent); }
        }

        .stats {
          display: grid;
          grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
          gap: 1rem;
          margin-bottom: 2rem;
        }

        .stat-card {
          background: var(--bg-card);
          border: 1px solid var(--border);
          border-radius: 0.75rem;
          padding: 1.25rem;
        }

        .stat-card .label {
          font-size: 0.75rem;
          color: var(--text-dim);
          text-transform: uppercase;
          letter-spacing: 0.05em;
          margin-bottom: 0.5rem;
        }

        .stat-card .value {
          font-family: 'JetBrains Mono', monospace;
          font-size: 2rem;
          font-weight: 700;
        }

        .stat-card .value.agents { color: var(--agent); }
        .stat-card .value.services { color: var(--service); }
        .stat-card .value.total { color: var(--accent); }

        .section-title {
          font-size: 0.875rem;
          font-weight: 600;
          color: var(--text-dim);
          text-transform: uppercase;
          letter-spacing: 0.08em;
          margin-bottom: 1rem;
          display: flex;
          align-items: center;
          gap: 0.5rem;
        }

        .section-title .dot {
          width: 8px;
          height: 8px;
          border-radius: 50%;
          display: inline-block;
        }

        .dot.agent-color { background: var(--agent); }
        .dot.service-color { background: var(--service); }

        .peer-grid {
          display: grid;
          grid-template-columns: repeat(auto-fill, minmax(320px, 1fr));
          gap: 1rem;
          margin-bottom: 2.5rem;
        }

        .peer-card {
          background: var(--bg-card);
          border: 1px solid var(--border);
          border-radius: 0.75rem;
          padding: 1.25rem;
          transition: all 0.2s ease;
        }

        .peer-card:hover {
          background: var(--bg-card-hover);
          border-color: var(--accent);
        }

        .peer-card .top {
          display: flex;
          justify-content: space-between;
          align-items: center;
          margin-bottom: 0.75rem;
        }

        .peer-card .peer-id {
          font-family: 'JetBrains Mono', monospace;
          font-size: 1rem;
          font-weight: 500;
        }

        .state-pill {
          font-family: 'JetBrains Mono', monospace;
          font-size: 0.65rem;
          padding: 0.15rem 0.5rem;
          border-radius: 9999px;
          text-transform: uppercase;
          letter-spacing: 0.05em;
        }

        .state-pill.idle { background: rgba(110,231,183,0.15); color: var(--idle); }
        .state-pill.busy { background: rgba(245,158,11,0.15); color: var(--busy); }
        .state-pill.error { background: rgba(239,68,68,0.15); color: var(--error); }
        .state-pill.shutting_down { background: rgba(75,85,99,0.15); color: var(--offline); }

        .peer-card .label-text {
          font-size: 0.8rem;
          color: var(--text-dim);
          margin-bottom: 0.75rem;
        }

        .peer-card .meta {
          font-family: 'JetBrains Mono', monospace;
          font-size: 0.7rem;
          color: var(--text-dim);
          display: grid;
          grid-template-columns: auto 1fr;
          gap: 0.25rem 0.75rem;
        }

        .peer-card .meta .k { color: var(--text-dim); }
        .peer-card .meta .v { color: var(--text); }

        .caps {
          display: flex;
          flex-wrap: wrap;
          gap: 0.35rem;
          margin-top: 0.75rem;
        }

        .cap-tag {
          font-family: 'JetBrains Mono', monospace;
          font-size: 0.6rem;
          padding: 0.1rem 0.4rem;
          border-radius: 4px;
          background: var(--accent-dim);
          color: var(--accent);
        }

        .empty-state {
          text-align: center;
          padding: 3rem;
          color: var(--text-dim);
          font-style: italic;
        }

        .event-log {
          background: var(--bg-card);
          border: 1px solid var(--border);
          border-radius: 0.75rem;
          padding: 1rem;
          max-height: 300px;
          overflow-y: auto;
          font-family: 'JetBrains Mono', monospace;
          font-size: 0.7rem;
          line-height: 1.6;
        }

        .event-log .entry {
          padding: 0.2rem 0;
          border-bottom: 1px solid rgba(30,30,46,0.5);
        }

        .event-log .ts { color: var(--text-dim); }
        .event-log .connected { color: var(--accent); }
        .event-log .disconnected { color: var(--error); }
        .event-log .state-change { color: var(--service); }

        @keyframes fadeIn {
          from { opacity: 0; transform: translateY(8px); }
          to { opacity: 1; transform: translateY(0); }
        }

        .peer-card { animation: fadeIn 0.3s ease; }
      </style>
      <meta name="csrf-token" content={Phoenix.Controller.get_csrf_token()} />
    </head>
    <body>
      {@inner_content}
      <script src="/cortex/assets/js/phoenix.min.js"></script>
      <script src="/cortex/assets/js/phoenix_live_view.min.js"></script>
      <script>
        let csrfToken = document.querySelector("meta[name='csrf-token']").getAttribute("content");
        let liveSocket = new window.LiveView.LiveSocket("/cortex/live", window.Phoenix.Socket, {
          params: { _csrf_token: csrfToken }
        });
        liveSocket.connect();
      </script>
    </body>
    </html>
    """
  end

  def app(assigns) do
    ~H"""
    <div class="container">
      {@inner_content}
    </div>
    """
  end
end
