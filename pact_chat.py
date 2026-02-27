"""Interactive chat client for the pact_admin MCP server.

The LLM understands natural-language requests, identifies which tool to call,
asks for any missing required parameters, confirms the full call with the user,
then executes it via the MCP server.

Usage:
    # Start the MCP server first:
    python3.10 -m pact_admin.mcp_server

    # Then in another terminal:
    python3.10 pact_chat.py
"""

import asyncio
import json
import sys

from mcp.client.streamable_http import streamablehttp_client
from mcp import ClientSession

import llm_client

MCP_URL = "http://127.0.0.1:8000/mcp"


# ---------------------------------------------------------------------------
# MCP helpers (async wrapped for synchronous use)
# ---------------------------------------------------------------------------

def _fetch_tools():
    """Connect to the MCP server and return the list of available tools."""
    async def _inner():
        async with streamablehttp_client(MCP_URL) as (read, write, _):
            async with ClientSession(read, write) as session:
                await session.initialize()
                return (await session.list_tools()).tools
    return asyncio.run(_inner())


def _call_tool(name, arguments):
    """Call a tool on the MCP server and return the result as a string."""
    async def _inner():
        async with streamablehttp_client(MCP_URL) as (read, write, _):
            async with ClientSession(read, write) as session:
                await session.initialize()
                result = await session.call_tool(name, arguments)
                if not result.content:
                    return "(no output)"
                # Check for tool-level errors
                if getattr(result, "isError", False):
                    return f"Error: {result.content[0].text}"
                return result.content[0].text
    return asyncio.run(_inner())


# ---------------------------------------------------------------------------
# System prompt construction
# ---------------------------------------------------------------------------

def _build_system_prompt(tools):
    """Build a system prompt that describes all tools and instructs JSON output."""
    tool_blocks = []
    for tool in tools:
        schema = tool.inputSchema or {}
        props = schema.get("properties", {})
        required = set(schema.get("required", []))

        param_lines = []
        for pname, pinfo in props.items():
            req_label = "required" if pname in required else "optional"
            ptype = pinfo.get("type", "any")
            desc = pinfo.get("description", "")
            default = pinfo.get("default")
            default_str = f"  default={default!r}" if default is not None else ""
            param_lines.append(f"    - {pname} ({req_label}, {ptype}{default_str}): {desc}")

        params_text = "\n".join(param_lines) if param_lines else "    (no parameters)"
        # Use only the first line of the description to keep the prompt compact
        short_desc = (tool.description or "").splitlines()[0]
        tool_blocks.append(
            f'Tool "{tool.name}": {short_desc}\n  Parameters:\n{params_text}'
        )

    tools_text = "\n\n".join(tool_blocks)

    return f"""\
You are a helpful assistant for the PACT outdoor module degradation analysis system.
You help users run administrative commands via natural-language requests.

Available tools:
{tools_text}

RESPONSE FORMAT — always reply with a single JSON object, nothing else:

1. Need more information (missing required parameters or ambiguous request):
   {{"action": "ask", "message": "<question for the user>"}}

2. All required parameters are known — present the call for user confirmation:
   {{"action": "confirm", "tool": "<tool_name>", "params": {{...}}, "message": "<plain-English summary of what will happen>"}}

3. User has confirmed (said yes / ok / go ahead / confirmed):
   {{"action": "run", "tool": "<tool_name>", "params": {{...}}}}

4. General question or no tool needed:
   {{"action": "chat", "message": "<response>"}}

Rules:
- Extract parameter values from the user's message whenever possible.
- Dates must be in YYYY-MM-DD format — ask the user to clarify if the format is ambiguous.
- Never run a tool without first presenting action="confirm" and receiving explicit approval.
- When the user says yes/ok/go/confirmed/do it/run it after a confirm, respond with action="run".
- When the user says no/cancel/stop after a confirm, respond with action="chat" explaining you cancelled.
- If a parameter has a default value you can infer, include it silently; only ask about missing required parameters.
- Keep messages concise and friendly.
- Respond ONLY with the JSON object — no markdown, no extra text.
"""


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------

def _format_confirmation(tool_name, params, message):
    params_str = "\n".join(f"  {k}: {v!r}" for k, v in params.items())
    return (
        f"{message}\n\n"
        f"Tool:   {tool_name}\n"
        f"Params:\n{params_str}\n\n"
        f"Proceed? (yes / no)"
    )


def _parse_llm_response(text):
    """Parse LLM response as JSON, stripping markdown fences if present."""
    clean = text.strip()
    if clean.startswith("```"):
        parts = clean.split("```")
        # parts[1] is the block content (possibly prefixed with 'json')
        block = parts[1]
        if block.startswith("json"):
            block = block[4:]
        clean = block.strip()
    return json.loads(clean)


# ---------------------------------------------------------------------------
# Main chat loop
# ---------------------------------------------------------------------------

def main():
    print("PACT Admin Chat")
    print("=" * 44)

    # Connect and fetch tools
    print("Connecting to MCP server at", MCP_URL, "...")
    try:
        tools = _fetch_tools()
    except Exception as exc:
        print(f"\nCould not connect to MCP server.")
        print(f"Make sure it is running:  python3.10 -m pact_admin.mcp_server")
        print(f"Details: {exc}")
        sys.exit(1)

    tool_names = [t.name for t in tools]
    print(f"Connected — {len(tools)} tools available: {', '.join(tool_names)}")

    # Validate LLM
    if not llm_client.validate_env():
        sys.exit(1)

    providers = llm_client.get_available_providers()
    model = llm_client.get_default_model()
    print(f"LLM provider: {providers[0]}  model: {model}")
    print("\nDescribe what you want to do, or type 'quit' to exit.\n")

    system_prompt = _build_system_prompt(tools)
    conversation = []          # list of {"role": ..., "content": ...}
    pending = None             # (tool_name, params) waiting for confirmation

    while True:
        try:
            user_input = input("You: ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nGoodbye.")
            break

        if not user_input:
            continue
        if user_input.lower() in ("quit", "exit", "q"):
            print("Goodbye.")
            break

        conversation.append({"role": "user", "content": user_input})

        messages = [{"role": "system", "content": system_prompt}] + conversation

        raw = llm_client.chat_completion(messages, max_tokens=1000, temperature=0.0)
        if raw is None:
            print("Assistant: (LLM error — no response)\n")
            conversation.pop()
            continue

        # Parse structured response
        try:
            resp = _parse_llm_response(raw)
        except json.JSONDecodeError:
            # LLM didn't follow instructions; display as plain text
            print(f"Assistant: {raw}\n")
            conversation.append({"role": "assistant", "content": raw})
            continue

        action = resp.get("action", "chat")

        if action == "ask":
            msg = resp.get("message", "")
            print(f"Assistant: {msg}\n")
            conversation.append({"role": "assistant", "content": raw})

        elif action == "confirm":
            tool_name = resp.get("tool", "")
            params = resp.get("params", {})
            message = resp.get("message", f"Ready to run {tool_name}.")
            pending = (tool_name, params)
            print(f"Assistant: {_format_confirmation(tool_name, params, message)}\n")
            conversation.append({"role": "assistant", "content": raw})

        elif action == "run":
            tool_name = resp.get("tool") or (pending[0] if pending else None)
            params = resp.get("params") or (pending[1] if pending else {})

            if not tool_name:
                print("Assistant: (nothing confirmed to run)\n")
                continue

            print(f"Running {tool_name}...\n")
            try:
                result = _call_tool(tool_name, params)
                print(f"Assistant: Done.\n\n{result}\n")
                conversation.append({
                    "role": "assistant",
                    "content": f"Ran {tool_name} with params {params}. Output:\n{result}",
                })
            except Exception as exc:
                print(f"Assistant: Error — {exc}\n")
                conversation.append({
                    "role": "assistant",
                    "content": f"Error running {tool_name}: {exc}",
                })
            pending = None

        else:  # "chat" or unknown
            msg = resp.get("message", raw)
            print(f"Assistant: {msg}\n")
            conversation.append({"role": "assistant", "content": raw})


if __name__ == "__main__":
    main()
