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

type MatchesResponse = {
  page?: number;
  page_size?: number;
  total?: number;
  items?: any[];
};

function pick(obj: any, keys: string[], fallback: any = null) {
  for (const k of keys) {
    const v = obj?.[k];
    if (v !== undefined && v !== null && v !== "") return v;
  }
  return fallback;
}

function toNumberSafe(v: any): number | null {
  if (v === null || v === undefined || v === "") return null;
  const n = Number(String(v).replace(",", "."));
  return Number.isFinite(n) ? n : null;
}

export default function MatchesPage() {
  const [page, setPage] = useState(1);
  const [pageSize, setPageSize] = useState(25);
  const [gtinMaxDist, setGtinMaxDist] = useState(15);
  const [maxImageDistance, setMaxImageDistance] = useState(8);

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
    return qs.toString();
  }, [page, pageSize, gtinMaxDist, maxImageDistance]);

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

  const total = data?.total ?? 0;
  const items = data?.items ?? [];
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

        <div className="grid grid-cols-1 gap-6 lg:grid-cols-[320px_1fr]">
          <Card className="h-fit">
            <CardHeader>
              <CardTitle>Filtros</CardTitle>
            </CardHeader>
            <CardContent className="space-y-4">
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

              <Separator />

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
                      <TableHead className="w-[140px]">Preço eBay</TableHead>
                      <TableHead className="w-[180px]">Gates</TableHead>
                      <TableHead className="w-[160px]">Links</TableHead>
                    </TableRow>
                  </TableHeader>

                  <TableBody>
                    {items.map((it: any, idx: number) => {
                      const asin = pick(it, ["asin", "amazon_asin"]);
                      const amazonTitle = pick(it, [
                        "amazon_title",
                        "title_amazon",
                        "amazon_product_title",
                        "title",
                      ]);
                      const amazonImg = pick(it, [
                        "amazon_image_url",
                        "image_url",
                        "amazon_image",
                      ]);
                      const ebayItemId = pick(it, ["ebay_item_id", "item_id", "ebay_id"]);
                      const ebayTitle = pick(it, ["ebay_title", "title_ebay"]);
                      const currency = pick(it, ["ebay_currency", "currency"], "USD");
                      const price = toNumberSafe(pick(it, ["ebay_price", "price"]));
                      const method = pick(it, ["match_method", "method"]);
                      const dist = toNumberSafe(pick(it, ["image_distance", "dist"]));

                      const amazonUrl = asin ? `https://www.amazon.com/dp/${asin}` : null;
                      const ebayUrl =
                        pick(it, ["ebay_url", "item_web_url", "web_url"]) ||
                        (ebayItemId ? `https://www.ebay.com/itm/${ebayItemId}` : null);

                      return (
                        <TableRow key={`${asin ?? "row"}-${idx}`}>
                          <TableCell>
                            {amazonImg ? (
                              <img
                                src={amazonImg}
                                alt={amazonTitle ?? asin ?? "imagem"}
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
                                {amazonTitle ?? ebayTitle ?? asin ?? "(sem título)"}
                              </div>
                              <div className="text-xs text-muted-foreground">
                                ASIN: <b>{asin ?? "-"}</b> • eBay: <b>{ebayItemId ?? "-"}</b>
                              </div>
                            </div>
                          </TableCell>

                          <TableCell>
                            <div className="text-sm font-semibold">
                              {price === null ? "-" : `${currency} ${price.toFixed(2)}`}
                            </div>
                            <div className="text-xs text-muted-foreground">
                              mais barata (por ASIN)
                            </div>
                          </TableCell>

                          <TableCell>
                            <div className="flex flex-wrap gap-2">
                              {method ? <Badge variant="secondary">{String(method)}</Badge> : null}
                              {dist !== null ? (
                                <Badge variant="outline">dist: {dist}</Badge>
                              ) : (
                                <Badge variant="outline">dist: null</Badge>
                              )}
                            </div>
                          </TableCell>

                          <TableCell>
                            <div className="flex flex-col gap-2">
                              {amazonUrl ? (
                                <Link className="text-sm underline underline-offset-4" href={amazonUrl} target="_blank">
                                  Amazon
                                </Link>
                              ) : (
                                <span className="text-sm text-muted-foreground">Amazon -</span>
                              )}
                              {ebayUrl ? (
                                <Link className="text-sm underline underline-offset-4" href={ebayUrl} target="_blank">
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
            </CardContent>
          </Card>
        </div>
      </div>
    </div>
  );
}