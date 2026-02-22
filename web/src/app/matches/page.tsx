"use client";

import Link from "next/link";
import { useEffect, useMemo, useState } from "react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Card, CardContent } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Separator } from "@/components/ui/separator";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";

type MatchItem = {
  created_at: string;

  match_method: string;
  match_score: number;
  image_distance: number | null;

  asin: string;
  amazon_title: string | null;
  amazon_brand: string | null;
  amazon_condition: string | null;
  amazon_price: number | null;
  amazon_currency: string | null;
  amazon_bsr: number | null;
  amazon_gtin: string | null;
  amazon_is_prime: number | null;
  amazon_fulfillment: string | null;
  amazon_browse_node_name: string | null;
  amazon_image_url: string | null;
  amazon_url: string | null;

  amazon_category_root?: string | null;
  amazon_category_child?: string | null;
  amazon_seller?: string | null;

  item_id: string;
  ebay_title: string | null;
  ebay_price: number | null;
  ebay_currency: string | null;
  ebay_condition: string | null;
  ebay_seller: string | null;
  ebay_url: string | null;

  spread?: number | null;
  spread_pct?: number | null;
};

type MatchesResponse = {
  page: number;
  page_size: number;
  total: number;
  items: MatchItem[];
};

type Filters = {
  palavraChave: string;
  categoria: string;
  subcategoria: string;

  precoAmazonMin: string;
  precoAmazonMax: string;
  somentePrime: boolean;
  logisticaAmazon: "QUALQUER" | "FBA" | "FBM";
  condicaoAmazon: "QUALQUER" | "NOVO" | "USADO" | "RECONDICIONADO" | "DESCONHECIDA";

  precoEbayMin: string;
  precoEbayMax: string;
  condicaoEbay: "QUALQUER" | "NOVO" | "USADO" | "RECONDICIONADO";

  ordenarPor: "recent" | "spread_desc" | "spread_pct_desc" | "ebay_price_asc" | "amazon_bsr_asc" | "match_score_desc";
};

const PAGE_SIZE = 50;

function parseNumberOrNull(v: string): number | null {
  const s = (v ?? "").trim();
  if (!s) return null;
  const n = Number(s.replace(",", "."));
  return Number.isFinite(n) ? n : null;
}

function fmtUSD(n: number | null, currency: string | null) {
  if (n == null) return "—";
  const cur = currency || "USD";
  try {
    return new Intl.NumberFormat("pt-BR", { style: "currency", currency: cur }).format(n);
  } catch {
    return `${cur} ${n.toFixed(2)}`;
  }
}

function mapAmazonCondPtToApi(v: Filters["condicaoAmazon"]): string | null {
  if (v === "QUALQUER") return null;
  if (v === "NOVO") return "NEW";
  if (v === "USADO") return "USED";
  if (v === "RECONDICIONADO") return "REFURB";
  if (v === "DESCONHECIDA") return "UNKNOWN";
  return null;
}

function mapEbayCondPtToApi(v: Filters["condicaoEbay"]): string | null {
  if (v === "QUALQUER") return null;
  if (v === "NOVO") return "NEW";
  if (v === "USADO") return "USED";
  if (v === "RECONDICIONADO") return "REFURB";
  return null;
}

function prettyCondPt(v: string | null) {
  const s = (v || "").toLowerCase();
  if (!s) return "—";
  if (s.startsWith("new")) return "Novo";
  if (s.startsWith("used")) return "Usado";
  if (s.includes("refurb") || s.includes("renew") || s.includes("recond")) return "Recondicionado";
  if (s === "new") return "Novo";
  if (s === "used") return "Usado";
  return v!;
}

function amazonFbaFbmLabel(v: string | null) {
  const s = (v || "").toUpperCase().trim();
  if (!s) return "—";
  if (["FBA", "AMAZON", "AFN"].includes(s)) return "FBA";
  if (["FBM", "MFN", "MERCHANT", "SELLER"].includes(s)) return "FBM";
  return s; // fallback
}

function cn(...classes: Array<string | false | null | undefined>) {
  return classes.filter(Boolean).join(" ");
}

export default function MatchesPage() {
  const [page, setPage] = useState(1);

  const [filters, setFilters] = useState<Filters>({
    palavraChave: "",
    categoria: "",
    subcategoria: "",

    precoAmazonMin: "",
    precoAmazonMax: "",
    somentePrime: false,
    logisticaAmazon: "QUALQUER",
    condicaoAmazon: "QUALQUER",

    precoEbayMin: "",
    precoEbayMax: "",
    condicaoEbay: "QUALQUER",

    ordenarPor: "recent",
  });

  const [applied, setApplied] = useState<Filters>(filters);

  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [data, setData] = useState<MatchesResponse>({
    page: 1,
    page_size: PAGE_SIZE,
    total: 0,
    items: [],
  });

  const query = useMemo(() => {
    const qs = new URLSearchParams();
    qs.set("page", String(page));
    qs.set("page_size", String(PAGE_SIZE));

    // padrões internos (usuário não controla)
    qs.set("gtin_max_dist", "15");
    qs.set("max_image_distance", "8");

    // filtros
    const apMin = parseNumberOrNull(applied.precoAmazonMin);
    const apMax = parseNumberOrNull(applied.precoAmazonMax);
    const ebMin = parseNumberOrNull(applied.precoEbayMin);
    const ebMax = parseNumberOrNull(applied.precoEbayMax);

    if (applied.palavraChave.trim()) qs.set("keyword", applied.palavraChave.trim());
    if (applied.categoria.trim()) qs.set("source_root_name", applied.categoria.trim());
    if (applied.subcategoria.trim()) qs.set("source_child_name", applied.subcategoria.trim());

    if (apMin != null) qs.set("amazon_price_min", String(apMin));
    if (apMax != null) qs.set("amazon_price_max", String(apMax));

    if (applied.somentePrime) qs.set("prime_only", "1");
    if (applied.logisticaAmazon !== "QUALQUER") qs.set("amazon_fulfillment", applied.logisticaAmazon);

    const ac = mapAmazonCondPtToApi(applied.condicaoAmazon);
    if (ac) qs.set("amazon_condition", ac);

    if (ebMin != null) qs.set("ebay_price_min", String(ebMin));
    if (ebMax != null) qs.set("ebay_price_max", String(ebMax));

    const ec = mapEbayCondPtToApi(applied.condicaoEbay);
    if (ec) qs.set("ebay_condition", ec);

    qs.set("sort", applied.ordenarPor);

    return qs.toString();
  }, [applied, page]);

  async function load() {
    setLoading(true);
    setError(null);
    try {
      const r = await fetch(`/api/matches?${query}`, { cache: "no-store" });
      if (!r.ok) throw new Error(`HTTP ${r.status}: ${await r.text()}`);
      const json = (await r.json()) as MatchesResponse;
      setData(json);
    } catch (e: any) {
      setError(String(e?.message ?? e));
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    load();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [query]);

  const total = data.total ?? 0;
  const totalPages = Math.max(1, Math.ceil(total / PAGE_SIZE));
  const items = data.items ?? [];

  const canPrev = page > 1;
  const canNext = page < totalPages;

  function applyFilters() {
    setPage(1);
    setApplied(filters);
  }

  function clearFilters() {
    const reset: Filters = {
      palavraChave: "",
      categoria: "",
      subcategoria: "",

      precoAmazonMin: "",
      precoAmazonMax: "",
      somentePrime: false,
      logisticaAmazon: "QUALQUER",
      condicaoAmazon: "QUALQUER",

      precoEbayMin: "",
      precoEbayMax: "",
      condicaoEbay: "QUALQUER",

      ordenarPor: "recent",
    };
    setPage(1);
    setFilters(reset);
    setApplied(reset);
  }

  return (
    <div className="min-h-screen overflow-x-hidden bg-[radial-gradient(1200px_circle_at_20%_0%,rgba(147,51,234,0.25),transparent_55%),radial-gradient(900px_circle_at_80%_10%,rgba(34,211,238,0.12),transparent_55%),radial-gradient(700px_circle_at_60%_90%,rgba(99,102,241,0.12),transparent_55%)] bg-[#06010a] text-slate-100">
      <div className="mx-auto max-w-7xl px-4 py-6">
        {/* Header */}
        <div className="mb-6 flex flex-col gap-3 md:flex-row md:items-end md:justify-between">
          <div>
            <h1 className="text-2xl font-semibold tracking-tight">
              Mineração de Produtos MinerEcom
            </h1>
          </div>

          <div className="flex items-center gap-2">
            <Badge className="border border-white/10 bg-white/5 text-slate-100">
              Encontrados: {total}
            </Badge>
            <Badge className="border border-white/10 bg-white/5 text-slate-100">
              Página {page} / {totalPages}
            </Badge>
          </div>
        </div>

        <div className="grid grid-cols-1 gap-4 lg:grid-cols-[340px_1fr]">
          {/* Sidebar filtros */}
          <Card className="border-white/10 bg-white/5 backdrop-blur-xl">
            <CardContent className="space-y-4 p-4">
              <div className="text-sm font-semibold text-slate-100">Filtros</div>
              <Separator className="bg-white/10" />

              <div className="space-y-2">
                <div className="text-xs text-slate-300">Palavra-chave</div>
                <Input
                  value={filters.palavraChave}
                  onChange={(e) => setFilters((f) => ({ ...f, palavraChave: e.target.value }))}
                  placeholder="ex: honda, sifangke..."
                  className="border-white/10 bg-white/5 text-slate-100 placeholder:text-slate-400"
                />
              </div>

              <div className="space-y-2">
                <div className="text-xs text-slate-300">Categoria</div>
                <Input
                  value={filters.categoria}
                  onChange={(e) => setFilters((f) => ({ ...f, categoria: e.target.value }))}
                  placeholder="ex: Casa & Cozinha"
                  className="border-white/10 bg-white/5 text-slate-100 placeholder:text-slate-400"
                />
              </div>

              <div className="space-y-2">
                <div className="text-xs text-slate-300">Subcategoria</div>
                <Input
                  value={filters.subcategoria}
                  onChange={(e) => setFilters((f) => ({ ...f, subcategoria: e.target.value }))}
                  placeholder="ex: Utensílios de Cozinha"
                  className="border-white/10 bg-white/5 text-slate-100 placeholder:text-slate-400"
                />
              </div>

              <Separator className="bg-white/10" />

              {/* Amazon */}
              <div className="text-xs font-semibold text-slate-200">Amazon</div>

              <div className="space-y-2">
                <div className="text-xs text-slate-300">Preço (mín / máx)</div>
                <div className="grid grid-cols-2 gap-2">
                  <Input
                    inputMode="decimal"
                    value={filters.precoAmazonMin}
                    onChange={(e) => setFilters((f) => ({ ...f, precoAmazonMin: e.target.value }))}
                    placeholder="mín"
                    className="border-white/10 bg-white/5 text-slate-100 placeholder:text-slate-400"
                  />
                  <Input
                    inputMode="decimal"
                    value={filters.precoAmazonMax}
                    onChange={(e) => setFilters((f) => ({ ...f, precoAmazonMax: e.target.value }))}
                    placeholder="máx"
                    className="border-white/10 bg-white/5 text-slate-100 placeholder:text-slate-400"
                  />
                </div>
              </div>

              <label className="flex items-center gap-2 text-sm text-slate-200">
                <input
                  type="checkbox"
                  checked={filters.somentePrime}
                  onChange={(e) => setFilters((f) => ({ ...f, somentePrime: e.target.checked }))}
                />
                Somente Prime
              </label>

              <div className="space-y-2">
                <div className="text-xs text-slate-300">Logística</div>
                <select
                  className="w-full rounded-md border border-white/10 bg-white/5 px-3 py-2 text-sm text-slate-100"
                  value={filters.logisticaAmazon}
                  onChange={(e) => setFilters((f) => ({ ...f, logisticaAmazon: e.target.value as any }))}
                >
                  <option value="QUALQUER">Qualquer</option>
                  <option value="FBA">FBA</option>
                  <option value="FBM">FBM</option>
                </select>
              </div>

              <div className="space-y-2">
                <div className="text-xs text-slate-300">Condição</div>
                <select
                  className="w-full rounded-md border border-white/10 bg-white/5 px-3 py-2 text-sm text-slate-100"
                  value={filters.condicaoAmazon}
                  onChange={(e) => setFilters((f) => ({ ...f, condicaoAmazon: e.target.value as any }))}
                >
                  <option value="QUALQUER">Qualquer</option>
                  <option value="NOVO">Novo</option>
                  <option value="USADO">Usado</option>
                  <option value="RECONDICIONADO">Recondicionado</option>
                  <option value="DESCONHECIDA">Desconhecida</option>
                </select>
              </div>

              <Separator className="bg-white/10" />

              {/* eBay */}
              <div className="text-xs font-semibold text-slate-200">eBay</div>

              <div className="space-y-2">
                <div className="text-xs text-slate-300">Preço (mín / máx)</div>
                <div className="grid grid-cols-2 gap-2">
                  <Input
                    inputMode="decimal"
                    value={filters.precoEbayMin}
                    onChange={(e) => setFilters((f) => ({ ...f, precoEbayMin: e.target.value }))}
                    placeholder="mín"
                    className="border-white/10 bg-white/5 text-slate-100 placeholder:text-slate-400"
                  />
                  <Input
                    inputMode="decimal"
                    value={filters.precoEbayMax}
                    onChange={(e) => setFilters((f) => ({ ...f, precoEbayMax: e.target.value }))}
                    placeholder="máx"
                    className="border-white/10 bg-white/5 text-slate-100 placeholder:text-slate-400"
                  />
                </div>
              </div>

              <div className="space-y-2">
                <div className="text-xs text-slate-300">Condição</div>
                <select
                  className="w-full rounded-md border border-white/10 bg-white/5 px-3 py-2 text-sm text-slate-100"
                  value={filters.condicaoEbay}
                  onChange={(e) => setFilters((f) => ({ ...f, condicaoEbay: e.target.value as any }))}
                >
                  <option value="QUALQUER">Qualquer</option>
                  <option value="NOVO">Novo</option>
                  <option value="USADO">Usado</option>
                  <option value="RECONDICIONADO">Recondicionado</option>
                </select>
              </div>

              <Separator className="bg-white/10" />

              <div className="space-y-2">
                <div className="text-xs text-slate-300">Ordenar por</div>
                <select
                  className="w-full rounded-md border border-white/10 bg-white/5 px-3 py-2 text-sm text-slate-100"
                  value={filters.ordenarPor}
                  onChange={(e) => setFilters((f) => ({ ...f, ordenarPor: e.target.value as any }))}
                >
                  <option value="recent">Mais recentes</option>
                  <option value="spread_desc">Maior diferença ($)</option>
                  <option value="spread_pct_desc">Maior diferença (%)</option>
                  <option value="ebay_price_asc">eBay mais barato</option>
                  <option value="amazon_bsr_asc">Melhor BSR (menor)</option>
                  <option value="match_score_desc">Maior score</option>
                </select>
              </div>

              <div className="grid grid-cols-2 gap-2 pt-2">
                <Button
                  onClick={applyFilters}
                  disabled={loading}
                  className="bg-violet-600/80 hover:bg-violet-600 text-white"
                >
                  Aplicar
                </Button>
                <Button
                  onClick={clearFilters}
                  disabled={loading}
                  variant="outline"
                  className="border-white/10 bg-white/5 text-slate-100 hover:bg-white/10"
                >
                  Limpar
                </Button>
              </div>

              {error && (
                <div className="rounded-md border border-red-500/30 bg-red-500/10 p-3 text-xs text-red-200">
                  {error}
                </div>
              )}
            </CardContent>
          </Card>

          {/* Conteúdo */}
          <Card className="min-w-0 border-white/10 bg-white/5 backdrop-blur-xl">
            <CardContent className="p-0">
              <div className="flex items-center justify-between gap-3 px-4 py-3">
                <div className="text-sm font-semibold text-slate-100">Produtos</div>

                <div className="flex items-center gap-2">
                  <Button
                    variant="outline"
                    className="border-white/10 bg-white/5 text-slate-100 hover:bg-white/10"
                    disabled={!canPrev || loading}
                    onClick={() => setPage((p) => Math.max(1, p - 1))}
                  >
                    ←
                  </Button>
                  <div className="text-xs text-slate-300">
                    Página <b className="text-slate-100">{page}</b> / {totalPages}
                  </div>
                  <Button
                    variant="outline"
                    className="border-white/10 bg-white/5 text-slate-100 hover:bg-white/10"
                    disabled={!canNext || loading}
                    onClick={() => setPage((p) => Math.min(totalPages, p + 1))}
                  >
                    →
                  </Button>
                </div>
              </div>

              <Separator className="bg-white/10" />

              {loading ? (
                <div className="px-4 py-6 text-sm text-slate-300">Carregando…</div>
              ) : items.length === 0 ? (
                <div className="px-4 py-10 text-sm text-slate-300">
                  Nenhum resultado com os filtros atuais.
                </div>
              ) : (
                <div className="min-w-0 overflow-x-hidden">
                  <Table className="w-full table-fixed">
                    <TableHeader>
                      <TableRow className="border-white/10">
                        <TableHead className="w-[420px] text-slate-200">Produto</TableHead>
                        <TableHead className="w-[240px] text-slate-200">Amazon</TableHead>
                        <TableHead className="w-[240px] text-slate-200">eBay</TableHead>
                        <TableHead className="w-[140px] text-slate-200">Links</TableHead>
                      </TableRow>
                    </TableHeader>

                    <TableBody>
                      {items.map((it) => {
                        const prime = it.amazon_is_prime === 1;
                        const fb = amazonFbaFbmLabel(it.amazon_fulfillment);
                        const cat = it.amazon_category_root || "—";
                        const sub = it.amazon_category_child || "—";

                        return (
                          <TableRow
                            key={`${it.asin}-${it.item_id}`}
                            className="border-white/10 transition-colors hover:bg-white/5"
                          >
                            {/* Produto */}
                            <TableCell className="py-2 align-top">
                              <div className="flex min-w-0 gap-3">
                                <div className="shrink-0">
                                  {it.amazon_image_url ? (
                                    <img
                                      src={it.amazon_image_url}
                                      alt={it.amazon_title ?? it.asin}
                                      className="h-11 w-11 rounded-md object-cover ring-1 ring-white/10"
                                      loading="lazy"
                                      referrerPolicy="no-referrer"
                                    />
                                  ) : (
                                    <div className="h-11 w-11 rounded-md bg-white/5 ring-1 ring-white/10" />
                                  )}
                                </div>

                                <div className="min-w-0">
                                  <div
                                    className="truncate text-sm font-medium text-slate-100"
                                    title={it.amazon_title ?? ""}
                                  >
                                    {it.amazon_title ?? it.ebay_title ?? it.asin}
                                  </div>

                                  <div className="mt-1 flex flex-wrap gap-2 text-xs text-slate-300">
                                    <span>
                                      ASIN: <b className="text-slate-100">{it.asin}</b>
                                    </span>
                                    <span>
                                      Marca: <b className="text-slate-100">{it.amazon_brand ?? "—"}</b>
                                    </span>
                                    <span>
                                      BSR: <b className="text-slate-100">{it.amazon_bsr ?? "—"}</b>
                                    </span>
                                  </div>

                                  <div className="mt-1 text-xs text-slate-300">
                                    Categoria: <b className="text-slate-100">{cat}</b> • Sub:{" "}
                                    <b className="text-slate-100">{sub}</b>
                                  </div>
                                </div>
                              </div>
                            </TableCell>

                            {/* Amazon */}
                            <TableCell className="py-2 align-top">
                              <div className="space-y-1 text-xs">
                                <div className="text-sm font-semibold text-slate-100">
                                  {fmtUSD(it.amazon_price, it.amazon_currency)}
                                </div>

                                <div className="text-slate-300">
                                  Condição: <b className="text-slate-100">{prettyCondPt(it.amazon_condition)}</b>
                                </div>

                                <div className="flex flex-wrap items-center gap-2">
                                  {prime ? (
                                    <Badge className="border border-emerald-400/20 bg-emerald-400/10 text-emerald-200">
                                      PRIME
                                    </Badge>
                                  ) : (
                                    <Badge className="border border-white/10 bg-white/5 text-slate-200">
                                      NÃO PRIME
                                    </Badge>
                                  )}

                                  <Badge className="border border-white/10 bg-white/5 text-slate-200">
                                    {fb}
                                  </Badge>
                                </div>

                                <div className="text-slate-300">
                                  Vendedor: <b className="text-slate-100">{it.amazon_seller ?? "—"}</b>
                                </div>
                              </div>
                            </TableCell>

                            {/* eBay */}
                            <TableCell className="py-2 align-top">
                              <div className="space-y-1 text-xs">
                                <div className="text-sm font-semibold text-slate-100">
                                  {fmtUSD(it.ebay_price, it.ebay_currency)}
                                </div>

                                <div className="text-slate-300">
                                  Condição: <b className="text-slate-100">{prettyCondPt(it.ebay_condition)}</b>
                                </div>

                                <div className="text-slate-300">
                                  Vendedor: <b className="text-slate-100">{it.ebay_seller ?? "—"}</b>
                                </div>
                              </div>
                            </TableCell>

                            {/* Links */}
                            <TableCell className="py-2 align-top">
                              <div className="flex flex-col gap-2">
                                <Button
                                  asChild
                                  variant="outline"
                                  className="h-8 border-white/10 bg-white/5 px-2 text-xs text-slate-100 hover:bg-white/10"
                                >
                                  <Link href={it.amazon_url ?? "#"} target="_blank">
                                    Amazon
                                  </Link>
                                </Button>

                                <Button
                                  asChild
                                  variant="outline"
                                  className="h-8 border-white/10 bg-white/5 px-2 text-xs text-slate-100 hover:bg-white/10"
                                >
                                  <Link href={it.ebay_url ?? "#"} target="_blank">
                                    eBay
                                  </Link>
                                </Button>
                              </div>
                            </TableCell>
                          </TableRow>
                        );
                      })}
                    </TableBody>
                  </Table>
                </div>
              )}

              <div className="flex items-center justify-between gap-3 px-4 py-3">
                <div className="text-xs text-slate-300">
                  {total > 0 ? `Mostrando ${items.length} itens nesta página.` : ""}
                </div>

                <div className="flex items-center gap-2">
                  <Button
                    variant="outline"
                    className="border-white/10 bg-white/5 text-slate-100 hover:bg-white/10"
                    disabled={!canPrev || loading}
                    onClick={() => setPage((p) => Math.max(1, p - 1))}
                  >
                    ← Voltar
                  </Button>
                  <Button
                    variant="outline"
                    className="border-white/10 bg-white/5 text-slate-100 hover:bg-white/10"
                    disabled={!canNext || loading}
                    onClick={() => setPage((p) => Math.min(totalPages, p + 1))}
                  >
                    Próxima →
                  </Button>
                </div>
              </div>
            </CardContent>
          </Card>
        </div>
      </div>
    </div>
  );
}