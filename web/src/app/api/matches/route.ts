import { NextResponse } from "next/server";

export async function GET(req: Request) {
  const base = process.env.API_BASE_URL;
  if (!base) {
    return NextResponse.json(
      { error: "API_BASE_URL não configurado em .env.local" },
      { status: 500 }
    );
  }

  const incoming = new URL(req.url);
  const upstream = new URL("/matches", base);

  incoming.searchParams.forEach((v, k) => upstream.searchParams.append(k, v));

  try {
    const r = await fetch(upstream.toString(), {
      headers: { accept: "application/json" },
      cache: "no-store",
    });

    const text = await r.text();
    try {
      return NextResponse.json(JSON.parse(text), { status: r.status });
    } catch {
      return new NextResponse(text, {
        status: r.status,
        headers: {
          "content-type":
            r.headers.get("content-type") ?? "text/plain; charset=utf-8",
        },
      });
    }
  } catch (e: any) {
    return NextResponse.json(
      { error: "Falha ao chamar API (FastAPI)", detail: String(e?.message ?? e) },
      { status: 502 }
    );
  }
}