---
title: Building an Embeddable Chat Widget
description: Step-by-step implementation of an embeddable floating chat widget for any website using HTML, Vanilla JS, and Firefly.
---

# Building an Embeddable Chat Widget

This guide provides a ready-to-use, embeddable floating chat widget that can be dropped into any HTML page, React app, Vue app, or CMS.

---

## 1. Embeddable HTML/JS Widget Code

You can include this single snippet directly in your web page's `<body>`:

```html
<!-- Firefly Chat Widget Container -->
<div id="firefly-widget-container">
  <!-- Floating Launch Button -->
  <button id="firefly-toggle-btn" aria-label="Open Chat">
    <svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
      <path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z"></path>
    </svg>
  </button>

  <!-- Chat Box Window -->
  <div id="firefly-chat-window" class="hidden">
    <div class="firefly-chat-header">
      <div class="firefly-chat-title">
        <span class="status-indicator"></span>
        <span>Firefly E2EE Chat</span>
      </div>
      <button id="firefly-close-btn">&times;</button>
    </div>
    <div id="firefly-messages-list"></div>
    <form id="firefly-input-form">
      <input type="text" id="firefly-input-text" placeholder="Type an encrypted message..." autocomplete="off" />
      <button type="submit" id="firefly-send-btn">Send</button>
    </form>
  </div>
</div>

<style>
  #firefly-widget-container {
    position: fixed;
    bottom: 24px;
    right: 24px;
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
    z-index: 99999;
  }
  #firefly-toggle-btn {
    width: 56px;
    height: 56px;
    border-radius: 50%;
    background: linear-gradient(135deg, #6366f1, #a855f7);
    color: white;
    border: none;
    cursor: pointer;
    box-shadow: 0 4px 14px rgba(99, 102, 241, 0.4);
    display: flex;
    align-items: center;
    justify-content: center;
    transition: transform 0.2s ease;
  }
  #firefly-toggle-btn:hover { transform: scale(1.05); }
  #firefly-chat-window {
    position: absolute;
    bottom: 70px;
    right: 0;
    width: 360px;
    height: 480px;
    background: #18181b;
    color: #fafafa;
    border-radius: 16px;
    box-shadow: 0 10px 30px rgba(0,0,0,0.4);
    border: 1px solid #27272a;
    display: flex;
    flex-direction: column;
    overflow: hidden;
  }
  #firefly-chat-window.hidden { display: none; }
  .firefly-chat-header {
    background: #27272a;
    padding: 12px 16px;
    display: flex;
    align-items: center;
    justify-content: space-between;
    font-weight: 600;
  }
  .status-indicator {
    display: inline-block;
    width: 8px;
    height: 8px;
    border-radius: 50%;
    background: #22c55e;
    margin-right: 8px;
  }
  #firefly-close-btn {
    background: transparent;
    border: none;
    color: #a1a1aa;
    font-size: 20px;
    cursor: pointer;
  }
  #firefly-messages-list {
    flex: 1;
    padding: 16px;
    overflow-y: auto;
    display: flex;
    flex-direction: column;
    gap: 8px;
  }
  .message-bubble {
    max-width: 80%;
    padding: 8px 12px;
    border-radius: 12px;
    font-size: 14px;
    word-break: break-word;
  }
  .message-bubble.incoming {
    align-self: flex-start;
    background: #27272a;
    color: #f4f4f5;
  }
  .message-bubble.outgoing {
    align-self: flex-end;
    background: #6366f1;
    color: white;
  }
  #firefly-input-form {
    display: flex;
    padding: 12px;
    gap: 8px;
    background: #27272a;
  }
  #firefly-input-text {
    flex: 1;
    background: #18181b;
    border: 1px solid #3f3f46;
    border-radius: 8px;
    padding: 8px 12px;
    color: white;
    outline: none;
  }
  #firefly-send-btn {
    background: #6366f1;
    color: white;
    border: none;
    padding: 8px 14px;
    border-radius: 8px;
    font-weight: 600;
    cursor: pointer;
  }
</style>

<script>
  (function initFireflyWidget() {
    const toggleBtn = document.getElementById('firefly-toggle-btn');
    const chatWindow = document.getElementById('firefly-chat-window');
    const closeBtn = document.getElementById('firefly-close-btn');
    const form = document.getElementById('firefly-input-form');
    const input = document.getElementById('firefly-input-text');
    const list = document.getElementById('firefly-messages-list');

    toggleBtn.addEventListener('click', () => chatWindow.classList.toggle('hidden'));
    closeBtn.addEventListener('click', () => chatWindow.classList.add('hidden'));

    function addMessage(text, type = 'incoming') {
      const bubble = document.createElement('div');
      bubble.className = `message-bubble ${type}`;
      bubble.textContent = text;
      list.appendChild(bubble);
      list.scrollTop = list.scrollHeight;
    }

    form.addEventListener('submit', (e) => {
      e.preventDefault();
      const text = input.value.trim();
      if (!text) return;
      
      addMessage(text, 'outgoing');
      input.value = '';

      // Post message to backend or Firefly client bridge
      window.dispatchEvent(new CustomEvent('firefly:send', { detail: { text } }));
    });

    // Listen for incoming messages from your client instance
    window.addEventListener('firefly:message', (e) => {
      if (e.detail?.text) {
        addMessage(e.detail.text, 'incoming');
      }
    });
  })();
</script>
```

---

## 2. React / Next.js Component

```tsx
import React, { useState, useEffect } from 'react';

export function FireflyChatWidget({ recipient = 'support_bot' }) {
  const [isOpen, setIsOpen] = useState(false);
  const [messages, setMessages] = useState<{ text: string; sender: 'user' | 'bot' }[]>([]);
  const [input, setInput] = useState('');

  const handleSend = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!input.trim()) return;

    setMessages((prev) => [...prev, { text: input, sender: 'user' }]);
    const sentText = input;
    setInput('');

    // Call your Firefly client bridge or API endpoint
    await fetch('/api/firefly/send', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ recipient, text: sentText }),
    });
  };

  return (
    <div className="fixed bottom-6 right-6 z-50">
      <button 
        onClick={() => setIsOpen(!isOpen)}
        className="w-14 h-14 rounded-full bg-indigo-600 text-white flex items-center justify-center shadow-lg"
      >
        💬
      </button>

      {isOpen && (
        <div className="absolute bottom-16 right-0 w-80 h-96 bg-zinc-900 text-white rounded-xl shadow-2xl flex flex-col p-4">
          <div className="flex justify-between items-center border-b border-zinc-700 pb-2 mb-2">
            <span className="font-semibold">Encrypted Chat (@{recipient})</span>
            <button onClick={() => setIsOpen(false)}>&times;</button>
          </div>
          
          <div className="flex-1 overflow-y-auto space-y-2">
            {messages.map((m, idx) => (
              <div key={idx} className={`p-2 rounded-lg text-sm ${m.sender === 'user' ? 'bg-indigo-600 ml-auto' : 'bg-zinc-800 mr-auto'}`}>
                {m.text}
              </div>
            ))}
          </div>

          <form onSubmit={handleSend} className="flex gap-2 mt-2">
            <input 
              value={input} 
              onChange={(e) => setInput(e.target.value)} 
              placeholder="Encrypted message..." 
              className="flex-1 bg-zinc-800 rounded px-2 py-1 text-sm outline-none"
            />
            <button type="submit" className="bg-indigo-600 px-3 py-1 rounded text-sm font-semibold">Send</button>
          </form>
        </div>
      )}
    </div>
  );
}
```
