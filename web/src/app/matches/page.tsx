"use client";

import Link from "next/link";
import { useEffect, useMemo, useState } from "react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
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

function toNumberOrNull(v: string): number | null {
  const s = (v ?? "").trim();
  if (!s) return null;
  const n = Number(s.replace(",", "."));
  return Number.isFinite(n) ? n : null;
}

function money(n: number | null, cur: string | null) {
  if (n === null || n === undefined) return "-";
  const c = cur || "USD";
  return `${c} ${n.toFixed(2)}`;
}

export default function MatchesPage() {
  // paginação
  const [page, setPage] = useState(1);
  const [pageSize, setPageSize] = useState(25);

  // regras de qualidade (gates)
  const [gtinMaxDist, setGtinMaxDist] = useState(15);
  const [maxImageDistance, setMaxImageDistance] = useState(8);

  // filtros (baseado no Streamlit)
  const [keyword, setKeyword] = useState("");
  const [amazonPriceMin, setAmazonPriceMin] = useState<string>("");
  const [amazonPriceMax, setAmazonPriceMax] = useState<string>("");
  const [primeOnly, setPrimeOnly] = useState(false);
  const [amazonFulfillment, setAmazonFulfillment] = useState<"ANY" | "FBA" | "FBM">("ANY");
  const [amazonCondition, setAmazonCondition] = useState<"ANY" | "NEW" | "USED" | "REFURB" | "UNKNOWN">("ANY");
  const [sourceRootName, setSourceRootName] = useState<string>("");
  const [sourceChildName, setSourceChildName] = useState<string>("");

  const [ebayPriceMin, setEbayPriceMin] = useState<string>("");
  const [ebayPriceMax, setEbayPriceMax] = useState<string>("");
  const [ebayCondition, setEbayCondition] = useState<"ANY" | "NEW" | "USED" | "REFURB">("ANY");

  const [includeMedia, setIncludeMedia] = useState(false);

  const [sort, setSort] = useState<
    "recent" | "spread_desc" | "spread_pct_desc" | "ebay_price_asc" | "amazon_bsr_asc" | "match_score_desc"
  >("recent");

  // state de dados
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [data, setData] = useState<MatchesResponse>({
    page: 1,
    page_size: 25,
    total: 0,
    items: [],
  });

  const query = useMemo(() => {
    const qs = new URLSearchParams();
    qs.set("page", String(page));
    qs.set("page_size", String(pageSize));

    qs.set("gtin_max_dist", String(gtinMaxDist));
    qs.set("max_image_distance", String(maxImageDistance));

    const apMin = toNumberOrNull(amazonPriceMin);
    const apMax = toNumberOrNull(amazonPriceMax);
    const ebMin = toNumberOrNull(ebayPriceMin);
    const ebMax = toNumberOrNull(ebayPriceMax);

    if (keyword.trim()) qs.set("keyword", keyword.trim());
    if (sourceRootName.trim()) qs.set("source_root_name", sourceRootName.trim());
    if (sourceChildName.trim()) qs.set("source_child_name", sourceChildName.trim());

    if (apMin !== null) qs.set("amazon_price_min", String(apMin));
    if (apMax !== null) qs.set("amazon_price_max", String(apMax));

    if (primeOnly) qs.set("prime_only", "1");
    if (amazonFulfillment !== "ANY") qs.set("amazon_fulfillment", amazonFulfillment);
    if (amazonCondition !== "ANY") qs.set("amazon_condition", amazonCondition);

    if (ebMin !== null) qs.set("ebay_price_min", String(ebMin));
    if (ebMax !== null) qs.set("ebay_price_max", String(ebMax));
    if (ebayCondition !== "ANY") qs.set("ebay_condition", ebayCondition);

    if (includeMedia) qs.set("include_media", "1");

    qs.set("sort", sort);

    return qs.toString();
  }, [
    page,
    pageSize,
    gtinMaxDist,
    maxImageDistance,
    keyword,
    sourceRootName,
    sourceChildName,
    amazonPriceMin,
    amazonPriceMax,
    primeOnly,
    amazonFulfillment,
    amazonCondition,
    ebayPriceMin,
    ebayPriceMax,
    ebayCondition,
    includeMedia,
    sort,
  ]);

  async function load() {
    setLoading(true);
    setError(null);

    try {
      const r = await fetch(`/api/matches?${query}`, { cache: "no-store" });
      if (!r.ok) {
        const t = await r.text();
        throw new Error(`HTTP ${r.status}: ${t}`);
      }
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
  const items = data.items ?? [];
  const totalPages = Math.max(1, Math.ceil(total / pageSize));

  return (
    <div className="min-h-screen p-6">
      <div className="mx-auto max-w-7xl">
        <div className="mb-6 flex items-center justify-between gap-4">
          <div>
            <h1 className="text-2xl font-semibold">Matches (Amazon ↔ eBay)</h1>
            <p className="text-sm text-muted-foreground">
              Oferta mais barata do eBay por ASIN (DB-first, confiável).
            </p>
          </div>

          <div className="flex items-center gap-2">
            <Badge variant="secondary">total: {total}</Badge>
            <Button variant="outline" onClick={load} disabled={loading}>
              Recarregar
            </Button>
          </div>
        </div>

        <div className="grid grid-cols-1 gap-6 lg:grid-cols-[340px_1fr]">
          {/* Sidebar */}
          <Card className="h-fit">
            <CardHeader>
              <CardTitle>Filtros</CardTitle>
            </CardHeader>
            <CardContent className="space-y-4">
              {/* keyword */}
              <div className="space-y-2">
                <div className="text-sm font-medium">keyword</div>
                <Input
                  value={keyword}
                  onChange={(e) => {
                    setPage(1);
                    setKeyword(e.target.value);
                  }}
                  placeholder="ex: honda, sifangke..."
                />
              </div>

              <Separator />

              {/* categoria/subcategoria */}
              <div className="space-y-2">
                <div className="text-sm font-medium">Categoria (root)</div>
                <Input
                  value={sourceRootName}
                  onChange={(e) => {
                    setPage(1);
                    setSourceRootName(e.target.value);
                  }}
                  placeholder="ex: Casa & Cozinha"
                />
              </div>

              <div className="space-y-2">
                <div className="text-sm font-medium">Subcategoria (child)</div>
                <Input
                  value={sourceChildName}
                  onChange={(e) => {
                    setPage(1);
                    setSourceChildName(e.target.value);
                  }}
                  placeholder="ex: Utensílios de Cozinha"
                />
              </div>

              <Separator />

              {/* Amazon */}
              <div className="space-y-2">
                <div className="text-sm font-medium">Preço Amazon (min/max)</div>
                <div className="grid grid-cols-2 gap-2">
                  <Input
                    inputMode="decimal"
                    value={amazonPriceMin}
                    onChange={(e) => {
                      setPage(1);
                      setAmazonPriceMin(e.target.value);
                    }}
                    placeholder="min"
                  />
                  <Input
                    inputMode="decimal"
                    value={amazonPriceMax}
                    onChange={(e) => {
                      setPage(1);
                      setAmazonPriceMax(e.target.value);
                    }}
                    placeholder="max"
                  />
                </div>
              </div>

              <div className="flex items-center gap-2">
                <input
                  type="checkbox"
                  checked={primeOnly}
                  onChange={(e) => {
                    setPage(1);
                    setPrimeOnly(e.target.checked);
                  }}
                />
                <span className="text-sm">Somente Prime</span>
              </div>

              <div className="space-y-2">
                <div className="text-sm font-medium">Fulfillment (Amazon)</div>
                <select
                  className="w-full rounded-md border px-3 py-2 text-sm"
                  value={amazonFulfillment}
                  onChange={(e) => {
                    setPage(1);
                    setAmazonFulfillment(e.target.value as any);
                  }}
                >
                  <option value="ANY">Qualquer</option>
                  <option value="FBA">FBA (Amazon)</option>
                  <option value="FBM">FBM (Seller)</option>
                </select>
              </div>

              <div className="space-y-2">
                <div className="text-sm font-medium">Condição (Amazon)</div>
                <select
                  className="w-full rounded-md border px-3 py-2 text-sm"
                  value={amazonCondition}
                  onChange={(e) => {
                    setPage(1);
                    setAmazonCondition(e.target.value as any);
                  }}
                >
                  <option value="ANY">Qualquer</option>
                  <option value="NEW">Novo</option>
                  <option value="USED">Usado</option>
                  <option value="REFURB">Recondicionado</option>
                  <option value="UNKNOWN">Desconhecida</option>
                </select>
              </div>

              <Separator />

              {/* eBay */}
              <div className="space-y-2">
                <div className="text-sm font-medium">Preço eBay (min/max)</div>
                <div className="grid grid-cols-2 gap-2">
                  <Input
                    inputMode="decimal"
                    value={ebayPriceMin}
                    onChange={(e) => {
                      setPage(1);
                      setEbayPriceMin(e.target.value);
                    }}
                    placeholder="min"
                  />
                  <Input
                    inputMode="decimal"
                    value={ebayPriceMax}
                    onChange={(e) => {
                      setPage(1);
                      setEbayPriceMax(e.target.value);
                    }}
                    placeholder="max"
                  />
                </div>
              </div>

              <div className="space-y-2">
                <div className="text-sm font-medium">Condição (eBay)</div>
                <select
                  className="w-full rounded-md border px-3 py-2 text-sm"
                  value={ebayCondition}
                  onChange={(e) => {
                    setPage(1);
                    setEbayCondition(e.target.value as any);
                  }}
                >
                  <option value="ANY">Qualquer</option>
                  <option value="NEW">Novo</option>
                  <option value="USED">Usado</option>
                  <option value="REFURB">Recondicionado</option>
                </select>
              </div>

              <Separator />

              {/* gates */}
              <div className="space-y-2">
                <div className="text-sm font-medium">gtin_max_dist</div>
                <Input
                  type="number"
                  min={0}
                  max={64}
                  value={gtinMaxDist}
                  onChange={(e) => {
                    setPage(1);
                    setGtinMaxDist(Number(e.target.value || 15));
                  }}
                />
                <p className="text-xs text-muted-foreground">
                  GTIN só passa se a imagem bater (distância ≤ limite).
                </p>
              </div>

              <div className="space-y-2">
                <div className="text-sm font-medium">max_image_distance</div>
                <Input
                  type="number"
                  min={0}
                  max={64}
                  value={maxImageDistance}
                  onChange={(e) => {
                    setPage(1);
                    setMaxImageDistance(Number(e.target.value || 8));
                  }}
                />
                <p className="text-xs text-muted-foreground">
                  Gate do match por imagem (quanto menor, mais rígido).
                </p>
              </div>

              <Separator />

              {/* sort */}
              <div className="space-y-2">
                <div className="text-sm font-medium">Ordenar por</div>
                <select
                  className="w-full rounded-md border px-3 py-2 text-sm"
                  value={sort}
                  onChange={(e) => {
                    setPage(1);
                    setSort(e.target.value as any);
                  }}
                >
                  <option value="recent">Mais recentes</option>
                  <option value="spread_desc">Maior spread ($)</option>
                  <option value="spread_pct_desc">Maior spread (%)</option>
                  <option value="ebay_price_asc">eBay mais barato</option>
                  <option value="amazon_bsr_asc">Melhor BSR (menor)</option>
                  <option value="match_score_desc">Maior score</option>
                </select>
              </div>

              <div className="flex items-center gap-2">
                <input
                  type="checkbox"
                  checked={includeMedia}
                  onChange={(e) => {
                    setPage(1);
                    setIncludeMedia(e.target.checked);
                  }}
                />
                <span className="text-sm">Incluir mídia (Movies/Books/etc.)</span>
              </div>

              <Separator />

              {/* paginação */}
              <div className="space-y-2">
                <div className="text-sm font-medium">page_size</div>
                <Input
                  type="number"
                  min={5}
                  max={200}
                  value={pageSize}
                  onChange={(e) => {
                    setPage(1);
                    setPageSize(Number(e.target.value || 25));
                  }}
                />
              </div>

              <div className="space-y-2">
                <div className="text-sm font-medium">Paginação</div>
                <div className="flex items-center gap-2">
                  <Button
                    variant="outline"
                    onClick={() => setPage((p) => Math.max(1, p - 1))}
                    disabled={loading || page <= 1}
                  >
                    ←
                  </Button>
                  <div className="text-sm">
                    Página <b>{page}</b> / {totalPages}
                  </div>
                  <Button
                    variant="outline"
                    onClick={() => setPage((p) => Math.min(totalPages, p + 1))}
                    disabled={loading || page >= totalPages}
                  >
                    →
                  </Button>
                </div>
              </div>

              {error && (
                <div className="rounded-md border border-red-500/30 bg-red-500/10 p-3 text-sm text-red-200">
                  {error}
                </div>
              )}
            </CardContent>
          </Card>

          {/* Main */}
          <Card>
            <CardHeader>
              <CardTitle>Resultados</CardTitle>
            </CardHeader>

            <CardContent>
              {loading ? (
                <div className="text-sm text-muted-foreground">Carregando…</div>
              ) : items.length === 0 ? (
                <div className="text-sm text-muted-foreground">
                  Nenhum resultado com os filtros atuais.
                </div>
              ) : (
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead className="w-[90px]">Imagem</TableHead>
                      <TableHead>Produto</TableHead>
                      <TableHead className="w-[140px]">Amazon</TableHead>
                      <TableHead className="w-[140px]">eBay</TableHead>
                      <TableHead className="w-[160px]">Spread</TableHead>
                      <TableHead className="w-[180px]">Gates</TableHead>
                      <TableHead className="w-[160px]">Links</TableHead>
                    </TableRow>
                  </TableHeader>

                  <TableBody>
                    {items.map((it) => {
                      return (
                        <TableRow key={`${it.asin}-${it.item_id}`}>
                          <TableCell>
                            {it.amazon_image_url ? (
                              <img
                                src={it.amazon_image_url}
                                alt={it.amazon_title ?? it.asin}
                                className="h-16 w-16 rounded-md object-cover"
                                loading="lazy"
                                referrerPolicy="no-referrer"
                              />
                            ) : (
                              <div className="h-16 w-16 rounded-md bg-muted" />
                            )}
                          </TableCell>

                          <TableCell>
                            <div className="space-y-1">
                              <div className="text-sm font-medium leading-snug">
                                {it.amazon_title ?? it.ebay_title ?? it.asin}
                              </div>
                              <div className="text-xs text-muted-foreground">
                                ASIN: <b>{it.asin}</b> • eBay: <b>{it.item_id}</b>
                              </div>
                              <div className="text-xs text-muted-foreground">
                                {it.amazon_brand ? <>Marca: <b>{it.amazon_brand}</b></> : null}
                                {it.amazon_browse_node_name ? <> • Node: <b>{it.amazon_browse_node_name}</b></> : null}
                              </div>
                            </div>
                          </TableCell>

                          <TableCell>
                            <div className="text-sm font-semibold">
                              {money(it.amazon_price, it.amazon_currency)}
                            </div>
                            <div className="text-xs text-muted-foreground">
                              {it.amazon_condition ?? "—"} • {it.amazon_is_prime === 1 ? "Prime" : "—"} • {it.amazon_fulfillment ?? "—"}
                            </div>
                          </TableCell>

                          <TableCell>
                            <div className="text-sm font-semibold">
                              {money(it.ebay_price, it.ebay_currency)}
                            </div>
                            <div className="text-xs text-muted-foreground">
                              {it.ebay_condition ?? "—"} • {it.ebay_seller ? `seller: ${it.ebay_seller}` : ""}
                            </div>
                          </TableCell>

                          <TableCell>
                            <div className="text-sm font-semibold">
                              {it.spread == null
  ? "-"
  : `${it.amazon_currency ?? it.ebay_currency ?? "USD"} ${Number(it.spread).toFixed(2)}`}

{it.spread_pct == null
  ? "-"
  : `${Number(it.spread_pct).toFixed(2)}%`}
                            </div>
                            <div className="text-xs text-muted-foreground">
                              {it.spread_pct == null
  ? "-"
  : `${Number(it.spread_pct).toFixed(2)}%`}
                            </div>
                          </TableCell>

                          <TableCell>
                            <div className="flex flex-wrap gap-2">
                              <Badge variant="secondary">{it.match_method}</Badge>
                              <Badge variant="outline">score: {it.match_score.toFixed(2)}</Badge>
                              <Badge variant="outline">
                                dist: {it.image_distance === null ? "null" : it.image_distance}
                              </Badge>
                            </div>
                          </TableCell>

                          <TableCell>
                            <div className="flex flex-col gap-2">
                              {it.amazon_url ? (
                                <Link className="text-sm underline underline-offset-4" href={it.amazon_url} target="_blank">
                                  Amazon
                                </Link>
                              ) : (
                                <span className="text-sm text-muted-foreground">Amazon -</span>
                              )}
                              {it.ebay_url ? (
                                <Link className="text-sm underline underline-offset-4" href={it.ebay_url} target="_blank">
                                  eBay
                                </Link>
                              ) : (
                                <span className="text-sm text-muted-foreground">eBay -</span>
                              )}
                            </div>
                          </TableCell>
                        </TableRow>
                      );
                    })}
                  </TableBody>
                </Table>
              )}

              <div className="mt-4 flex items-center justify-between text-sm text-muted-foreground">
                <div>
                  Página <b>{page}</b> de {totalPages} • {items.length} itens nesta página
                </div>
                <div>
                  (Oferta selecionada = eBay mais barato por ASIN)
                </div>
              </div>
            </CardContent>
          </Card>
        </div>
      </div>
    </div>
  );
}