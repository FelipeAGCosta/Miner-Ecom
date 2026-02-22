"use client";

import Link from "next/link";
import { useEffect, useMemo, useState } from "react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Card, CardContent } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Separator } from "@/components/ui/separator";
import { Checkbox } from "@/components/ui/checkbox";
import { Label } from "@/components/ui/label";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { Menu, X, User, CreditCard, BadgeCheck, Search, ChevronsLeft, ChevronsRight, ChevronLeft, ChevronRight } from "lucide-react";

type CategoryTree = {
  categories: Array<{ name: string; children: string[] }>;
};

type MatchItem = {
  asin: string;
  amazon_title: string | null;
  amazon_brand: string | null;
  amazon_condition: string | null;
  amazon_price: number | null;
  amazon_currency: string | null;
  amazon_bsr: number | null;
  amazon_is_prime: number | null;
  amazon_fulfillment: string | null;
  amazon_image_url: string | null;
  amazon_url: string | null;
  amazon_category_root?: string | null;
  amazon_category_child?: string | null;

  item_id: string;
  ebay_price: number | null;
  ebay_currency: string | null;
  ebay_condition: string | null;
  ebay_seller: string | null;
  ebay_url: string | null;
};

type MatchesResponse = {
  page: number;
  page_size: number;
  total: number;
  items: MatchItem[];
};

type Filters = {
  palavraChave: string;
  categoria: string; // "__ALL__" = todas
  subcategoria: string; // "__ALL__" = todas

  precoAmazonMin: string;
  precoAmazonMax: string;
  somentePrime: boolean;
  logisticaAmazon: "QUALQUER" | "FBA" | "FBM";
  condicaoAmazon: "QUALQUER" | "NOVO" | "USADO" | "RECONDICIONADO" | "DESCONHECIDA";

  // (placeholder UI) Quantidade de vendas nos últimos 30 dias (mínimo)
  amazon_sales_30d_min: string;

  precoEbayMin: string;
  precoEbayMax: string;
  condicaoEbay: "QUALQUER" | "NOVO" | "USADO" | "RECONDICIONADO";

  // (placeholder UI) Quantidade mínima em estoque
  ebay_stock_min: string;

  ordenarPor:
    | "recent"
    | "spread_desc"
    | "spread_pct_desc"
    | "ebay_price_asc"
    | "amazon_bsr_asc"
    | "match_score_desc";
};

const PAGE_SIZE = 10;
const GTIN_MAX_DIST = 15;
const MAX_IMAGE_DISTANCE = 8;

function parseNumberOrNull(v: string): number | null {
  const s = (v ?? "").trim();
  if (!s) return null;
  const n = Number(s.replace(",", "."));
  return Number.isFinite(n) ? n : null;
}

function fmtMoney(n: number | null, currency: string | null) {
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
  return v!;
}

function amazonFbaFbmLabel(v: string | null) {
  const s = (v || "").toUpperCase().trim();
  if (!s) return "—";
  if (["FBA", "AMAZON", "AFN"].includes(s)) return "FBA";
  if (["FBM", "MFN", "MERCHANT", "SELLER"].includes(s)) return "FBM";
  return s;
}

function cn(...classes: Array<string | false | null | undefined>) {
  return classes.filter(Boolean).join(" ");
}

function makeEbaySearchUrl(q: string) {
  const kw = encodeURIComponent((q || "").trim());
  return `https://www.ebay.com/sch/i.html?_nkw=${kw}`;
}

export default function MatchesPage() {
  const [page, setPage] = useState(1);
  const [navOpen, setNavOpen] = useState(false);

  const [filters, setFilters] = useState<Filters>({
    palavraChave: "",
    categoria: "__ALL__",
    subcategoria: "__ALL__",

    precoAmazonMin: "",
    precoAmazonMax: "",
    somentePrime: false,
    logisticaAmazon: "QUALQUER",
    condicaoAmazon: "QUALQUER",

    amazon_sales_30d_min: "",

    precoEbayMin: "",
    precoEbayMax: "",
    condicaoEbay: "QUALQUER",

    ebay_stock_min: "",

    ordenarPor: "recent",
  });

  const [applied, setApplied] = useState<Filters>(filters);

  const [catTree, setCatTree] = useState<CategoryTree>({ categories: [] });
  const [catLoading, setCatLoading] = useState(false);

  const [data, setData] = useState<MatchesResponse>({
    page: 1,
    page_size: PAGE_SIZE,
    total: 0,
    items: [],
  });

  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const rootOptions = catTree.categories;
  const selectedRoot = filters.categoria === "__ALL__" ? "" : filters.categoria;

  const rootChildrenRaw = useMemo(() => {
    const found = rootOptions.find((c) => c.name === selectedRoot);
    return found?.children ?? [];
  }, [rootOptions, selectedRoot]);

  // remove entradas inválidas (ex: "-" que estava aparecendo)
  const rootChildren = useMemo(() => {
    const cleaned = rootChildrenRaw
      .map((x) => (x ?? "").toString())
      .map((x) => x.trim())
      .filter((x) => x && x !== "-" && x !== "—");
    return Array.from(new Set(cleaned));
  }, [rootChildrenRaw]);

  // reset subcategoria quando troca categoria
  useEffect(() => {
    if (filters.categoria === "__ALL__") {
      if (filters.subcategoria !== "__ALL__") setFilters((f) => ({ ...f, subcategoria: "__ALL__" }));
      return;
    }
    if (filters.subcategoria !== "__ALL__" && !rootChildren.includes(filters.subcategoria)) {
      setFilters((f) => ({ ...f, subcategoria: "__ALL__" }));
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [filters.categoria, rootChildren.join("|")]);

  async function loadCategories() {
    setCatLoading(true);
    try {
      const r = await fetch("/api/filters/categories", { cache: "no-store" });
      if (!r.ok) throw new Error(await r.text());
      const json = (await r.json()) as CategoryTree;
      setCatTree(json);
    } catch {
      setCatTree({ categories: [] });
    } finally {
      setCatLoading(false);
    }
  }

  useEffect(() => {
    loadCategories();
  }, []);

  const query = useMemo(() => {
    const qs = new URLSearchParams();
    qs.set("page", String(page));
    qs.set("page_size", String(PAGE_SIZE));

    // defaults internos
    qs.set("gtin_max_dist", String(GTIN_MAX_DIST));
    qs.set("max_image_distance", String(MAX_IMAGE_DISTANCE));

    const apMin = parseNumberOrNull(applied.precoAmazonMin);
    const apMax = parseNumberOrNull(applied.precoAmazonMax);
    const ebMin = parseNumberOrNull(applied.precoEbayMin);
    const ebMax = parseNumberOrNull(applied.precoEbayMax);

    const amazonSales30dMin = parseNumberOrNull(applied.amazon_sales_30d_min);
    const ebayStockMin = parseNumberOrNull(applied.ebay_stock_min);

    if (applied.palavraChave.trim()) qs.set("keyword", applied.palavraChave.trim());

    if (applied.categoria !== "__ALL__") qs.set("source_root_name", applied.categoria);
    if (applied.subcategoria !== "__ALL__") qs.set("source_child_name", applied.subcategoria);

    if (apMin != null) qs.set("amazon_price_min", String(apMin));
    if (apMax != null) qs.set("amazon_price_max", String(apMax));

    if (applied.somentePrime) qs.set("prime_only", "1");
    if (applied.logisticaAmazon !== "QUALQUER") qs.set("amazon_fulfillment", applied.logisticaAmazon);

    const ac = mapAmazonCondPtToApi(applied.condicaoAmazon);
    if (ac) qs.set("amazon_condition", ac);

    // placeholders (backend ainda não usa; seguros para já deixar no front)
    if (amazonSales30dMin != null) qs.set("amazon_sales_30d_min", String(amazonSales30dMin));

    if (ebMin != null) qs.set("ebay_price_min", String(ebMin));
    if (ebMax != null) qs.set("ebay_price_max", String(ebMax));

    const ec = mapEbayCondPtToApi(applied.condicaoEbay);
    if (ec) qs.set("ebay_condition", ec);

    // placeholder (backend ainda não usa; seguro para já deixar no front)
    if (ebayStockMin != null) qs.set("ebay_stock_min", String(ebayStockMin));

    qs.set("sort", applied.ordenarPor);
    return qs.toString();
  }, [applied, page]);

  async function loadMatches() {
    setLoading(true);
    setError(null);
    try {
      const r = await fetch(`/api/matches?${query}`, { cache: "no-store" });
      if (!r.ok) throw new Error(`HTTP ${r.status}: ${await r.text()}`);
      const json = (await r.json()) as MatchesResponse;
      setData(json);
    } catch (e: any) {
      setError(String(e?.message ?? e));
      setData({ page: 1, page_size: PAGE_SIZE, total: 0, items: [] });
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    loadMatches();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [query]);

  const total = data.total ?? 0;
  const items = data.items ?? [];
  const totalPages = Math.max(1, Math.ceil(total / PAGE_SIZE));
  const canPrev = page > 1;
  const canNext = page < totalPages;

  function applyFilters() {
    setPage(1);
    setApplied(filters);
  }

  function clearFilters() {
    const reset: Filters = {
      palavraChave: "",
      categoria: "__ALL__",
      subcategoria: "__ALL__",

      precoAmazonMin: "",
      precoAmazonMax: "",
      somentePrime: false,
      logisticaAmazon: "QUALQUER",
      condicaoAmazon: "QUALQUER",

      amazon_sales_30d_min: "",

      precoEbayMin: "",
      precoEbayMax: "",
      condicaoEbay: "QUALQUER",

      ebay_stock_min: "",

      ordenarPor: "recent",
    };
    setPage(1);
    setFilters(reset);
    setApplied(reset);
  }

  function goFirst() {
    setPage(1);
  }
  function goLast() {
    setPage(Math.max(1, totalPages));
  }
  function goPrev10() {
    setPage((p) => Math.max(1, p - 10));
  }
  function goNext10() {
    setPage((p) => Math.min(Math.max(1, totalPages), p + 10));
  }

  const inputCls =
    "border-white/10 bg-white/5 text-slate-100 placeholder:text-slate-400 transition-all duration-150 hover:border-white/20 focus-visible:ring-2 focus-visible:ring-indigo-400/35 focus-visible:ring-offset-0";
  const triggerCls =
    "border-white/10 bg-white/5 text-slate-100 cursor-pointer transition-all duration-150 hover:border-white/20 hover:bg-white/10 focus:ring-2 focus:ring-indigo-400/35 focus:ring-offset-0";
  const outlineBtnCls =
    "border-white/10 bg-white/5 text-slate-100 hover:bg-white/10 hover:border-white/20 transition-all duration-150 cursor-pointer";
  const iconBtnCls =
    "h-10 w-10 px-0 grid place-items-center";

  const navItems = [
    { label: "Minerar", icon: Search, href: "/matches", enabled: true },
    { label: "Perfil", icon: User, href: "/perfil", enabled: false },
    { label: "Assinatura", icon: CreditCard, href: "/assinatura", enabled: false },
    { label: "Aprovações", icon: BadgeCheck, href: "/aprovacoes", enabled: false },
  ];

  return (
    <div className="min-h-screen w-full overflow-x-hidden bg-[#0b1020] text-slate-100">
      {/* Side nav */}
      <div
        className={cn(
          "fixed inset-y-0 left-0 z-40 w-[260px] border-r border-white/10 bg-[#0a0f1d] transition-transform duration-300",
          navOpen ? "translate-x-0" : "-translate-x-full"
        )}
      >
        <div className="flex items-center justify-between px-4 py-4">
          <div className="flex items-center gap-3">
            <div className="grid h-10 w-10 place-items-center rounded-xl bg-white/5 ring-1 ring-white/10">
              <span className="text-sm font-extrabold text-indigo-200">ME</span>
            </div>
            <div className="leading-tight">
              <div className="text-base font-semibold text-slate-100">
                Miner<span className="text-indigo-200">Ecom</span>
              </div>
              <div className="text-xs text-slate-400">Menu</div>
            </div>
          </div>

          <Button
            variant="outline"
            className={cn(outlineBtnCls, iconBtnCls, "group")}
            onClick={() => setNavOpen(false)}
            aria-label="Fechar menu"
          >
            <X className="h-4 w-4 text-slate-100 opacity-95 transition-transform duration-150 group-hover:scale-105" />
          </Button>
        </div>

        <Separator className="bg-white/10" />

        <div className="p-3">
          <div className="space-y-1">
            {navItems.map((it) => {
              const Icon = it.icon;
              const base =
                "flex items-center justify-between rounded-xl px-3 py-2 transition-all duration-150";
              const left =
                "flex items-center gap-2 text-sm";
              const enabledCls =
                "cursor-pointer hover:bg-white/5 hover:ring-1 hover:ring-white/10";
              const disabledCls =
                "opacity-60 cursor-not-allowed";

              const content = (
                <div className={cn(base, it.enabled ? enabledCls : disabledCls)}>
                  <div className={left}>
                    <Icon className="h-4 w-4 text-slate-300" />
                    <span className="text-slate-100">{it.label}</span>
                  </div>
                  {!it.enabled && (
                    <span className="text-[10px] text-slate-400">Em breve</span>
                  )}
                </div>
              );

              return it.enabled ? (
                <Link
                  key={it.label}
                  href={it.href}
                  onClick={() => setNavOpen(false)}
                >
                  {content}
                </Link>
              ) : (
                <div key={it.label}>{content}</div>
              );
            })}
          </div>
        </div>
      </div>

      {/* Overlay */}
      {navOpen && (
        <div
          className="fixed inset-0 z-30 bg-black/50"
          onClick={() => setNavOpen(false)}
          aria-hidden="true"
        />
      )}

      <div
        className={cn(
          "w-full px-2 py-4 transition-[padding] duration-300",
          navOpen ? "lg:pl-[276px]" : "lg:pl-2"
        )}
      >
        {/* Header */}
        <div className="mb-4 grid grid-cols-[340px_1fr_220px] items-center gap-3">
          {/* Brand + hamburger */}
          <div className="flex items-center gap-3">
            <Button
              variant="outline"
              className={cn(outlineBtnCls, iconBtnCls, "group")}
              onClick={() => setNavOpen((v) => !v)}
              aria-label="Abrir menu"
            >
              <Menu className="h-4 w-4 text-slate-100 opacity-95 transition-transform duration-150 group-hover:scale-105" />
            </Button>

            <div className="grid h-10 w-10 place-items-center rounded-xl bg-white/5 ring-1 ring-white/10 shadow-[0_10px_30px_rgba(0,0,0,0.35)] transition-transform duration-200 hover:scale-[1.02]">
              <span className="text-sm font-extrabold text-indigo-200">ME</span>
            </div>

            <div className="leading-tight">
              <div className="text-base font-semibold tracking-wide text-slate-100">
                Miner<span className="text-indigo-200">Ecom</span>
              </div>
              <div className="text-xs text-slate-400">Minerar</div>
            </div>
          </div>

          {/* Title */}
          <div className="text-center">
            <h1 className="text-2xl font-semibold tracking-tight">
              <span className="text-slate-100">
                Mineração de Produtos <span className="text-indigo-200">MinerEcom</span>
              </span>
            </h1>
            <div className="mt-1 text-xs text-slate-400">
              Encontre produtos com match exato Amazon ↔ eBay
            </div>
          </div>

          {/* Found count */}
          <div className="flex justify-end">
            <Badge className="border border-white/10 bg-white/5 text-slate-100">
              Encontrados: {total}
            </Badge>
          </div>
        </div>

        <div className="grid grid-cols-1 gap-3 lg:grid-cols-[320px_1fr]">
          {/* Filters */}
          <Card className="h-fit border-white/10 bg-white/5 lg:sticky lg:top-3">
            <CardContent className="space-y-4 p-4">
              <div className="text-base font-semibold text-slate-100 text-center">Filtros</div>
              <Separator className="bg-white/10" />

              <div className="space-y-2">
                <div className="text-xs text-slate-300">
                  Palavra-chave{" "}
                  <span className="text-slate-400">(Recomendação: escreva em inglês)</span>
                </div>
                <Input
                  value={filters.palavraChave}
                  onChange={(e) => setFilters((f) => ({ ...f, palavraChave: e.target.value }))}
                  placeholder="ex: kitchen brush, patio, lawn..."
                  className={cn(inputCls, "cursor-text")}
                />
              </div>

              {/* Categoria/Subcategoria */}
              <div className="space-y-2">
                <div className="text-xs text-slate-300">Categoria</div>
                <Select value={filters.categoria} onValueChange={(v) => setFilters((f) => ({ ...f, categoria: v }))}>
                  <SelectTrigger className={cn(triggerCls, "group")}>
                    <SelectValue placeholder={catLoading ? "Carregando..." : "Todas"} />
                  </SelectTrigger>
                  <SelectContent className="border-white/10 bg-[#0a0f1d] text-slate-100">
                    <SelectItem value="__ALL__">Todas</SelectItem>
                    {rootOptions.map((c) => (
                      <SelectItem key={c.name} value={c.name}>
                        {c.name}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>

              <div className="space-y-2">
                <div className="text-xs text-slate-300">Subcategoria</div>
                <Select
                  value={filters.subcategoria}
                  onValueChange={(v) => setFilters((f) => ({ ...f, subcategoria: v }))}
                  disabled={filters.categoria === "__ALL__"}
                >
                  <SelectTrigger className={cn(triggerCls, filters.categoria === "__ALL__" && "opacity-60 cursor-not-allowed")}>
                    <SelectValue placeholder={filters.categoria === "__ALL__" ? "Selecione uma categoria" : "Todas"} />
                  </SelectTrigger>
                  <SelectContent className="border-white/10 bg-[#0a0f1d] text-slate-100">
                    <SelectItem value="__ALL__">Todas</SelectItem>
                    {rootChildren.map((ch) => (
                      <SelectItem key={ch} value={ch}>
                        {ch}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>

              <Separator className="bg-white/10" />

              {/* Amazon box */}
              <div className="rounded-xl border border-white/10 bg-white/5 p-3">
                <div className="mb-2 flex items-center justify-between">
                  <div className="text-xs font-semibold text-slate-100">Filtros Amazon</div>
                  <Badge className="border border-white/10 bg-white/5 text-slate-100">Amazon</Badge>
                </div>

                <div className="space-y-2">
                  <div className="text-xs text-slate-300">Preço (mínimo / máximo)</div>
                  <div className="grid grid-cols-2 gap-2">
                    <Input
                      inputMode="decimal"
                      value={filters.precoAmazonMin}
                      onChange={(e) => setFilters((f) => ({ ...f, precoAmazonMin: e.target.value }))}
                      placeholder="mín"
                      className={cn(inputCls, "cursor-text")}
                    />
                    <Input
                      inputMode="decimal"
                      value={filters.precoAmazonMax}
                      onChange={(e) => setFilters((f) => ({ ...f, precoAmazonMax: e.target.value }))}
                      placeholder="máx"
                      className={cn(inputCls, "cursor-text")}
                    />
                  </div>
                </div>

                <div className="mt-3 flex items-center gap-2">
                  <Checkbox
                    id="primeOnly"
                    checked={filters.somentePrime}
                    onCheckedChange={(v) => setFilters((f) => ({ ...f, somentePrime: Boolean(v) }))}
                  />
                  <Label
                    htmlFor="primeOnly"
                    className="text-sm text-slate-200 cursor-pointer select-none"
                  >
                    Somente Prime
                  </Label>
                </div>

                <div className="mt-2 space-y-2">
                  <div className="text-xs text-slate-300">Logística</div>
                  <Select value={filters.logisticaAmazon} onValueChange={(v) => setFilters((f) => ({ ...f, logisticaAmazon: v as any }))}>
                    <SelectTrigger className={triggerCls}>
                      <SelectValue placeholder="Qualquer" />
                    </SelectTrigger>
                    <SelectContent className="border-white/10 bg-[#0a0f1d] text-slate-100">
                      <SelectItem value="QUALQUER">Qualquer</SelectItem>
                      <SelectItem value="FBA">FBA</SelectItem>
                      <SelectItem value="FBM">FBM</SelectItem>
                    </SelectContent>
                  </Select>
                </div>

                <div className="mt-2 space-y-2">
                  <div className="text-xs text-slate-300">Condição</div>
                  <Select value={filters.condicaoAmazon} onValueChange={(v) => setFilters((f) => ({ ...f, condicaoAmazon: v as any }))}>
                    <SelectTrigger className={triggerCls}>
                      <SelectValue placeholder="Qualquer" />
                    </SelectTrigger>
                    <SelectContent className="border-white/10 bg-[#0a0f1d] text-slate-100">
                      <SelectItem value="QUALQUER">Qualquer</SelectItem>
                      <SelectItem value="NOVO">Novo</SelectItem>
                      <SelectItem value="USADO">Usado</SelectItem>
                      <SelectItem value="RECONDICIONADO">Recondicionado</SelectItem>
                      <SelectItem value="DESCONHECIDA">Desconhecida</SelectItem>
                    </SelectContent>
                  </Select>
                </div>

                {/* Placeholder: Vendas 30 dias */}
                <div className="mt-2 space-y-2">
                  <div className="text-xs text-slate-300">Quantidade de vendas nos últimos 30 dias:</div>
                  <Input
                    inputMode="numeric"
                    value={filters.amazon_sales_30d_min}
                    onChange={(e) => setFilters((f) => ({ ...f, amazon_sales_30d_min: e.target.value }))}
                    placeholder="mín"
                    className={cn(inputCls, "cursor-text")}
                  />
                </div>
              </div>

              {/* eBay box */}
              <div className="rounded-xl border border-white/10 bg-white/5 p-3">
                <div className="mb-2 flex items-center justify-between">
                  <div className="text-xs font-semibold text-slate-100">Filtros eBay</div>
                  <Badge className="border border-white/10 bg-white/5 text-slate-100">eBay</Badge>
                </div>

                <div className="space-y-2">
                  <div className="text-xs text-slate-300">Preço (mínimo / máximo)</div>
                  <div className="grid grid-cols-2 gap-2">
                    <Input
                      inputMode="decimal"
                      value={filters.precoEbayMin}
                      onChange={(e) => setFilters((f) => ({ ...f, precoEbayMin: e.target.value }))}
                      placeholder="mín"
                      className={cn(inputCls, "cursor-text")}
                    />
                    <Input
                      inputMode="decimal"
                      value={filters.precoEbayMax}
                      onChange={(e) => setFilters((f) => ({ ...f, precoEbayMax: e.target.value }))}
                      placeholder="máx"
                      className={cn(inputCls, "cursor-text")}
                    />
                  </div>
                </div>

                <div className="mt-2 space-y-2">
                  <div className="text-xs text-slate-300">Condição</div>
                  <Select value={filters.condicaoEbay} onValueChange={(v) => setFilters((f) => ({ ...f, condicaoEbay: v as any }))}>
                    <SelectTrigger className={triggerCls}>
                      <SelectValue placeholder="Qualquer" />
                    </SelectTrigger>
                    <SelectContent className="border-white/10 bg-[#0a0f1d] text-slate-100">
                      <SelectItem value="QUALQUER">Qualquer</SelectItem>
                      <SelectItem value="NOVO">Novo</SelectItem>
                      <SelectItem value="USADO">Usado</SelectItem>
                      <SelectItem value="RECONDICIONADO">Recondicionado</SelectItem>
                    </SelectContent>
                  </Select>
                </div>

                {/* Placeholder: Estoque mínimo */}
                <div className="mt-2 space-y-2">
                  <div className="text-xs text-slate-300">Quantidade mínima em estoque:</div>
                  <Input
                    inputMode="numeric"
                    value={filters.ebay_stock_min}
                    onChange={(e) => setFilters((f) => ({ ...f, ebay_stock_min: e.target.value }))}
                    placeholder="mín"
                    className={cn(inputCls, "cursor-text")}
                  />
                </div>
              </div>

              <Separator className="bg-white/10" />

              <div className="space-y-2">
                <div className="text-xs text-slate-300">Ordenar por</div>
                <Select value={filters.ordenarPor} onValueChange={(v) => setFilters((f) => ({ ...f, ordenarPor: v as any }))}>
                  <SelectTrigger className={triggerCls}>
                    <SelectValue placeholder="Mais recentes" />
                  </SelectTrigger>
                  <SelectContent className="border-white/10 bg-[#0a0f1d] text-slate-100">
                    <SelectItem value="recent">Mais recentes</SelectItem>
                    <SelectItem value="spread_desc">Maior diferença ($)</SelectItem>
                    <SelectItem value="spread_pct_desc">Maior diferença (%)</SelectItem>
                    <SelectItem value="ebay_price_asc">eBay mais barato</SelectItem>
                    <SelectItem value="amazon_bsr_asc">Melhor BSR (menor)</SelectItem>
                    <SelectItem value="match_score_desc">Maior score</SelectItem>
                  </SelectContent>
                </Select>
              </div>

              <div className="grid grid-cols-2 gap-2 pt-2">
                <Button
                  onClick={applyFilters}
                  disabled={loading}
                  className="bg-indigo-500 hover:bg-indigo-400 text-white transition-all duration-150 hover:shadow-[0_10px_25px_rgba(99,102,241,0.18)] cursor-pointer"
                >
                  Aplicar
                </Button>
                <Button
                  onClick={clearFilters}
                  disabled={loading}
                  variant="outline"
                  className={cn(
                    "border-white/10 bg-white/5 text-slate-100 hover:bg-white/10 transition-all duration-150 cursor-pointer",
                    "hover:border-white/20"
                  )}
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

          {/* Results */}
          <Card className="min-w-0 border-white/10 bg-white/5">
            <CardContent className="p-0">
              <div className="grid grid-cols-[1fr_auto_1fr] items-center px-4 py-3">
                <div />
                <div className="text-sm font-semibold text-slate-100 text-center"></div>

                <div className="flex items-center justify-end gap-2">
                  <div className="text-xs text-slate-300 mr-1">
                    Página <b className="text-slate-100">{page}</b> / {Math.max(1, totalPages)}
                  </div>

                  <div className="flex items-center gap-1">
                    <Button
                      variant="outline"
                      className={cn(outlineBtnCls, "h-9 w-9 p-0 grid place-items-center")}
                      disabled={page <= 1 || loading}
                      onClick={goFirst}
                      aria-label="Ir para primeira página"
                      title="Primeira"
                    >
                      <ChevronsLeft className="h-4 w-4 text-slate-100 opacity-95" />
                    </Button>

                    <Button
                      variant="outline"
                      className={cn(outlineBtnCls, "h-9 w-9 p-0 grid place-items-center")}
                      disabled={page <= 1 || loading}
                      onClick={goPrev10}
                      aria-label="Voltar 10 páginas"
                      title="Voltar 10"
                    >
                      <ChevronsLeft className="h-4 w-4 text-slate-100 opacity-95 -translate-x-[1px]" />
                    </Button>

                    <Button
                      variant="outline"
                      className={cn(outlineBtnCls, "h-9 w-9 p-0 grid place-items-center")}
                      disabled={!canPrev || loading}
                      onClick={() => setPage((p) => Math.max(1, p - 1))}
                      aria-label="Página anterior"
                      title="Anterior"
                    >
                      <ChevronLeft className="h-4 w-4 text-slate-100 opacity-95" />
                    </Button>

                    <Button
                      variant="outline"
                      className={cn(outlineBtnCls, "h-9 w-9 p-0 grid place-items-center")}
                      disabled={!canNext || loading}
                      onClick={() => setPage((p) => Math.min(Math.max(1, totalPages), p + 1))}
                      aria-label="Próxima página"
                      title="Próxima"
                    >
                      <ChevronRight className="h-4 w-4 text-slate-100 opacity-95" />
                    </Button>

                    <Button
                      variant="outline"
                      className={cn(outlineBtnCls, "h-9 w-9 p-0 grid place-items-center")}
                      disabled={page >= totalPages || loading}
                      onClick={goNext10}
                      aria-label="Avançar 10 páginas"
                      title="Avançar 10"
                    >
                      <ChevronsRight className="h-4 w-4 text-slate-100 opacity-95 translate-x-[1px]" />
                    </Button>

                    <Button
                      variant="outline"
                      className={cn(outlineBtnCls, "h-9 w-9 p-0 grid place-items-center")}
                      disabled={page >= totalPages || loading}
                      onClick={goLast}
                      aria-label="Ir para última página"
                      title="Última"
                    >
                      <ChevronsRight className="h-4 w-4 text-slate-100 opacity-95" />
                    </Button>
                  </div>
                </div>
              </div>

              <Separator className="bg-white/10" />

              {error && (
                <div className="px-4 pt-4 text-sm text-red-200">
                  Erro ao buscar dados: <span className="text-red-100">{error}</span>
                </div>
              )}

              {loading ? (
                <div className="px-4 py-6 text-sm text-slate-300">Carregando…</div>
              ) : items.length === 0 ? (
                <div className="px-4 py-10 text-sm text-slate-300">
                  Nenhum resultado com os filtros atuais.
                  <div className="mt-2 text-xs text-slate-400">
                    Dica: clique em <b className="text-slate-200">Limpar</b> e depois{" "}
                    <b className="text-slate-200">Aplicar</b>.
                  </div>
                </div>
              ) : (
                <div className="min-w-0 overflow-x-hidden">
                  <Table className="w-full table-fixed">
                    <TableHeader>
                      <TableRow className="border-white/10">
                        <TableHead className="w-[560px] text-slate-200">Produto</TableHead>
                        <TableHead className="w-[320px] text-slate-200 text-center">Amazon</TableHead>
                        <TableHead className="w-[320px] text-slate-200 text-center">eBay</TableHead>
                        <TableHead className="w-[140px] text-slate-200 text-center">Links</TableHead>
                      </TableRow>
                    </TableHeader>

                    <TableBody>
                      {items.map((it) => {
                        const prime = it.amazon_is_prime === 1;
                        const fb = amazonFbaFbmLabel(it.amazon_fulfillment);

                        const badgePrime = prime ? (
                          <Badge className="border border-emerald-400/20 bg-emerald-400/10 text-emerald-200">
                            PRIME
                          </Badge>
                        ) : (
                          <Badge className="border border-red-400/20 bg-red-400/10 text-red-200">
                            NÃO PRIME
                          </Badge>
                        );

                        const badgeFB =
                          fb === "FBA" ? (
                            <Badge className="border border-sky-400/20 bg-sky-400/10 text-sky-200">
                              FBA
                            </Badge>
                          ) : fb === "FBM" ? (
                            <Badge className="border border-amber-400/25 bg-amber-400/10 text-amber-200">
                              FBM
                            </Badge>
                          ) : (
                            <Badge className="border border-white/10 bg-white/5 text-slate-200">
                              {fb}
                            </Badge>
                          );

                        const cat = it.amazon_category_root || "—";
                        const sub = it.amazon_category_child || "—";

                        const offersQuery = `${it.amazon_brand ?? ""} ${it.amazon_title ?? it.asin}`.trim();
                        const ebayOffersUrl = makeEbaySearchUrl(offersQuery);

                        return (
                          <TableRow
                            key={`${it.asin}-${it.item_id}`}
                            className="border-white/10 transition-colors hover:bg-white/5"
                          >
                            <TableCell className="py-3 align-top">
                              <div className="flex min-w-0 gap-4">
                                <div className="shrink-0">
                                  {it.amazon_image_url ? (
                                    <img
                                      src={it.amazon_image_url}
                                      alt={it.amazon_title ?? it.asin}
                                      className="h-24 w-24 rounded-lg object-cover ring-1 ring-white/10"
                                      loading="lazy"
                                      referrerPolicy="no-referrer"
                                    />
                                  ) : (
                                    <div className="h-24 w-24 rounded-lg bg-white/5 ring-1 ring-white/10" />
                                  )}
                                </div>

                                <div className="min-w-0">
                                  <div
                                    className="truncate text-sm font-semibold text-slate-100"
                                    title={it.amazon_title ?? ""}
                                  >
                                    {it.amazon_title ?? it.asin}
                                  </div>

                                  <div className="mt-2 flex flex-wrap gap-3 text-xs text-slate-300">
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

                                  <div className="mt-2 text-xs text-slate-300">
                                    Categoria: <b className="text-slate-100">{cat}</b> • Sub:{" "}
                                    <b className="text-slate-100">{sub}</b>
                                  </div>
                                </div>
                              </div>
                            </TableCell>

                            <TableCell className="py-3 align-top">
                              <div className="space-y-3 text-xs text-center">
                                <div className="text-sm font-semibold text-slate-100">
                                  Amazon: {fmtMoney(it.amazon_price, it.amazon_currency)}
                                </div>
                                <div className="text-slate-300">
                                  Condição:{" "}
                                  <b className="text-slate-100">{prettyCondPt(it.amazon_condition)}</b>
                                </div>
                                <div className="flex flex-wrap gap-2 justify-center">
                                  {badgePrime}
                                  {badgeFB}
                                </div>
                              </div>
                            </TableCell>

                            <TableCell className="py-3 align-top">
                              <div className="space-y-3 text-xs text-center">
                                <div className="text-sm font-semibold text-slate-100">
                                  eBay: {fmtMoney(it.ebay_price, it.ebay_currency)}
                                </div>
                                <div className="text-slate-300">
                                  Condição:{" "}
                                  <b className="text-slate-100">{prettyCondPt(it.ebay_condition)}</b>
                                </div>
                                <div className="text-slate-300">
                                  Vendedor: <b className="text-slate-100">{it.ebay_seller ?? "—"}</b>
                                </div>
                              </div>
                            </TableCell>

                            <TableCell className="py-3 align-top">
                              <div className="flex flex-col gap-2">
                                <Button
                                  asChild
                                  variant="outline"
                                  className={cn(
                                    "h-9 border-white/10 bg-white/5 px-2 text-xs text-slate-100 hover:bg-white/10 hover:border-white/20 transition-all duration-150 cursor-pointer",
                                    !it.amazon_url && "opacity-50 pointer-events-none"
                                  )}
                                >
                                  <Link href={it.amazon_url ?? "#"} target="_blank">
                                    Amazon
                                  </Link>
                                </Button>

                                <Button
                                  asChild
                                  variant="outline"
                                  className={cn(
                                    "h-9 border-white/10 bg-white/5 px-2 text-xs text-slate-100 hover:bg-white/10 hover:border-white/20 transition-all duration-150 cursor-pointer",
                                    !it.ebay_url && "opacity-50 pointer-events-none"
                                  )}
                                >
                                  <Link href={it.ebay_url ?? "#"} target="_blank">
                                    eBay
                                  </Link>
                                </Button>

                                <Button
                                  asChild
                                  variant="outline"
                                  className="h-9 border-indigo-400/20 bg-indigo-400/10 px-2 text-xs font-bold text-indigo-100 hover:bg-indigo-400/15 hover:border-indigo-300/30 transition-all duration-150 cursor-pointer"
                                >
                                  <Link href={ebayOffersUrl} target="_blank">
                                    + Ofertas
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
            </CardContent>
          </Card>
        </div>
      </div>
    </div>
  );
}