import { NextResponse } from "next/server";

export async function GET() {
  const base = process.env.API_BASE_URL;
  if (!base) {
    return NextResponse.json(
      { error: "API_BASE_URL não configurado em .env.local" },
      { status: 500 }
    );
  }

  const upstream = new URL("/filters/categories", base);

  try {
    const r = await fetch(upstream.toString(), {
      headers: { accept: "application/json" },
      cache: "no-store",
    });

    const text = await r.text();
    try {
      return NextResponse.json(JSON.parse(text), { status: r.status });
    } catch {
      return new NextResponse(text, { status: r.status });
    }
  } catch (e: any) {
    return NextResponse.json(
      { error: "Falha ao chamar API (FastAPI)", detail: String(e?.message ?? e) },
      { status: 502 }
    );
  }
}