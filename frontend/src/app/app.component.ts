import { Component, OnDestroy, computed, signal } from '@angular/core';
import { DecimalPipe } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { LeafletDirective } from '@bluehalo/ngx-leaflet';
import * as L from 'leaflet';
import type { Feature, FeatureCollection, Point } from 'geojson';

type FeedMode = 'live' | 'replay';
type AppMode = 'demo' | 'api';
type MagnitudeBand = 'nd' | 'low' | 'moderate' | 'strong' | 'major';
type ComparisonDirection = 'above' | 'below' | 'even' | 'unknown';
type EventProperties = {
  mag: number | null;
  place: string | null;
  time: number;
  depth?: number | null;
  depth_km?: number | null;
  country_id?: string | null;
  country_name?: string | null;
};
type Earthquake = Feature<Point, EventProperties>;
const DIRECTIONS_ES: Record<string, string> = {
  N: 'norte',
  NNE: 'norte-noreste',
  NE: 'noreste',
  ENE: 'este-noreste',
  E: 'este',
  ESE: 'este-sureste',
  SE: 'sureste',
  SSE: 'sur-sureste',
  S: 'sur',
  SSW: 'sur-suroeste',
  SW: 'suroeste',
  WSW: 'oeste-suroeste',
  W: 'oeste',
  WNW: 'oeste-noroeste',
  NW: 'noroeste',
  NNW: 'norte-noroeste'
};
const COUNTRY_NAMES_ES: Record<string, string> = {
  ethiopia: 'Etiopía',
  japan: 'Japón',
  mexico: 'México',
  taiwan: 'Taiwán'
};
type EventMetadata = {
  generated?: number;
  last_ingestion?: number | null;
  mode?: FeedMode;
  count?: number;
  total?: number;
  truncated?: boolean;
  next_cursor?: number | null;
};
type Events = FeatureCollection<Point, EventProperties> & { metadata?: EventMetadata };
type Config = {
  mode: AppMode;
  apiBaseUrl: string;
  refreshMs: number;
};
type Filters = {
  start: string;
  end: string;
  minMag: string;
  countryId: string;
  mode: FeedMode;
};
type CountRow = { count: number };
type CountryCount = {
  country_id: string | null;
  country_name: string;
  continent: string | null;
  subregion: string;
  count: number;
};
type Distribution = {
  unit: string;
  null_count: number;
  excluded_nulls: number;
  bins: { range: string; count: number }[];
};
type ApiStatistics = {
  metadata: {
    generated: number;
    mode: FeedMode;
    start: string | null;
    end: string | null;
    min_mag: number | null;
    country_id: string | null;
    total: number;
    complete: boolean;
  };
  events_by_country: CountryCount[];
  events_by_hour: { hour: string; count: number }[];
  magnitude_distribution: Distribution;
  depth_distribution: Distribution;
  historical_comparison: {
    unit: string;
    minimum_magnitude: number;
    live_last_24h: { start: string; end: string; count: number };
    replay_2025: { start: string; end: string; days: number; count: number; daily_average: number };
    includes_zero_event_days: boolean;
  };
};
type ApiHealth = {
  status: string;
  last_ingestion: number | null;
  ingestion_age_seconds: number | null;
  ingestion_stale: boolean;
};
type CountryOption = { id: string; name: string };
type HourCount = { hour: number; label: string; count: number };

const REPLAY_START = '2025-01-01T00:00';
const REPLAY_END = '2026-01-01T00:00';
const HOUR_MS = 60 * 60 * 1000;

@Component({
  selector: 'app-root',
  standalone: true,
  imports: [DecimalPipe, FormsModule, LeafletDirective],
  templateUrl: './app.component.html',
  styleUrl: './app.component.css'
})
export class AppComponent implements OnDestroy {
  readonly appMode = signal<AppMode>('api');
  readonly filters = signal<Filters>({
    start: '',
    end: '',
    minMag: '',
    countryId: '',
    mode: 'live'
  });
  readonly status = signal('Preparando el mapa…');
  readonly apiError = signal('');
  readonly lastRead = signal('Sin lectura');
  readonly generated = signal('Sin información');
  readonly lastIngestion = signal<number | null>(null);
  readonly ingestionAge = signal<number | null>(null);
  readonly ingestionStale = signal(false);
  readonly readingTimestamp = signal<number | null>(null);
  readonly ageTick = signal(Date.now());
  readonly statistics = signal<ApiStatistics | null>(null);
  readonly events = signal<Earthquake[]>([]);
  readonly countryOptions = signal<CountryOption[]>([]);
  readonly countriesStatus = signal('Cargando catálogo');
  readonly period = signal('A la espera de la primera consulta');
  readonly appliedFilters = signal('Sin consulta válida');
  readonly totalEvents = signal(0);
  readonly nextCursor = signal<number | null>(null);
  readonly omitted = signal(0);
  readonly refreshing = signal(false);
  readonly stale = computed(() => this.ingestionStale());
  readonly subregionCounts = computed(() => {
    const grouped = new Map<string, number>();
    for (const row of this.statistics()?.events_by_country ?? []) {
      grouped.set(row.subregion, (grouped.get(row.subregion) ?? 0) + row.count);
    }
    return [...grouped.entries()]
      .map(([name, count]) => ({ name, count }))
      .sort((a, b) => b.count - a.count || a.name.localeCompare(b.name));
  });
  readonly hourlyWindowsTotal = computed(() => {
    const stats = this.statistics();
    if (!stats) return 0;
    const start = stats.metadata.start ? Date.parse(stats.metadata.start) : Number.NaN;
    const end = stats.metadata.end ? Date.parse(stats.metadata.end) : Number.NaN;
    if (!Number.isFinite(start) || !Number.isFinite(end) || end <= start) {
      return stats.events_by_hour.length;
    }
    const firstHour = Math.floor(start / HOUR_MS) * HOUR_MS;
    const endHour = Math.ceil(end / HOUR_MS) * HOUR_MS;
    return Math.max(0, Math.ceil((endHour - firstHour) / HOUR_MS));
  });
  readonly hourlyCounts = computed<HourCount[]>(() => {
    const stats = this.statistics();
    if (!stats) return [];
    const counts = new Map(stats.events_by_hour.map(row => [Date.parse(row.hour), row.count]));
    const start = stats.metadata.start ? Date.parse(stats.metadata.start) : Number.NaN;
    const end = stats.metadata.end ? Date.parse(stats.metadata.end) : Number.NaN;
    if (!Number.isFinite(start) || !Number.isFinite(end) || end <= start) {
      return stats.events_by_hour.slice(-168).map(row => {
        const hour = Date.parse(row.hour);
        return { hour, label: this.formatHour(hour), count: row.count };
      });
    }
    const firstHour = Math.floor(start / HOUR_MS) * HOUR_MS;
    const endHour = Math.ceil(end / HOUR_MS) * HOUR_MS;
    const displayStart = Math.max(firstHour, endHour - 168 * HOUR_MS);
    const result: HourCount[] = [];
    for (let hour = displayStart; hour < endHour; hour += HOUR_MS) {
      result.push({
        hour,
        label: this.formatHour(hour),
        count: counts.get(hour) ?? 0
      });
    }
    return result;
  });
  readonly historicalVariation = computed<number | null>(() => {
    const comparison = this.statistics()?.historical_comparison;
    if (!comparison || comparison.replay_2025.daily_average === 0) return null;
    return ((comparison.live_last_24h.count - comparison.replay_2025.daily_average)
      / comparison.replay_2025.daily_average) * 100;
  });
  readonly historicalVariationDirection = computed<ComparisonDirection>(() => {
    const variation = this.historicalVariation();
    return variation === null ? 'unknown' : variation > 0 ? 'above' : variation < 0 ? 'below' : 'even';
  });
  readonly historicalVariationWidth = computed(() => Math.min(100, Math.abs(this.historicalVariation() ?? 0)));
  readonly options: L.MapOptions = {
    center: L.latLng(18, 0),
    zoom: 2,
    minZoom: 2,
    maxZoom: 8,
    preferCanvas: true,
    worldCopyJump: false
  };

  private config?: Config;
  private map?: L.Map;
  private eventsLayer = L.layerGroup();
  private pollTimer?: ReturnType<typeof setTimeout>;
  private debounceTimer?: ReturnType<typeof setTimeout>;
  private ageTimer?: ReturnType<typeof setInterval>;
  private activeRequest?: AbortController;
  private readonly lifetime = new AbortController();
  private requestId = 0;
  private destroyed = false;

  constructor() {
    // Actualiza el reloj visible para calcular la antigüedad de las lecturas e ingestas.
    this.ageTimer = setInterval(() => this.ageTick.set(Date.now()), 30000);
  }

  async onMapReady(map: L.Map): Promise<void> {
    // Prepara capas, catálogo geográfico y configuración antes de consultar los datos.
    this.map = map;
    const base = L.tileLayer('https://tile.openstreetmap.org/{z}/{x}/{y}.png', {
      maxZoom: 19,
      noWrap: true,
      attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
    }).addTo(map);
    const borders = L.geoJSON(undefined, {
      style: { color: '#475569', weight: 0.9, opacity: 0.8, fill: false },
      onEachFeature: (feature, layer) => {
        const name = document.createElement('span');
        name.textContent = feature.properties?.NAME_ES ?? feature.properties?.ADMIN ?? 'Sin nombre';
        layer.bindTooltip(name);
      }
    }).addTo(map);
    this.eventsLayer.addTo(map);
    L.control.layers(
      { OpenStreetMap: base },
      { 'Países (Natural Earth)': borders, Terremotos: this.eventsLayer }
    ).addTo(map);
    map.attributionControl.addAttribution('Fronteras: Natural Earth · Datos: USGS');

    try {
      const countries = await this.readJson<FeatureCollection>('datos/paises.geojson', {}, this.lifetime.signal);
      borders.addData(countries);
      this.countryOptions.set(this.getCountryOptions(countries));
      this.countriesStatus.set('Catálogo de países cargado');
    } catch (error) {
      if (!this.lifetime.signal.aborted) {
        this.countriesStatus.set(`No se pudo cargar el catálogo de países: ${this.errorMessage(error)}`);
      }
    }
    if (this.destroyed) return;

    try {
      const config = await this.readJson<Config>('config.json', {}, this.lifetime.signal);
      this.validateConfig(config);
      this.config = config;
      this.appMode.set(config.mode);
      this.status.set(config.mode === 'demo'
        ? 'DEMOSTRACIÓN · datos ficticios; no representa actividad real'
        : 'API · conectando con el servidor');
      await this.refresh();
    } catch (error) {
      if (!this.destroyed) {
        const message = `No se pudo cargar la configuración: ${this.errorMessage(error)}`;
        this.status.set(message);
        this.apiError.set(message);
      }
    }
  }

  setStart(value: string): void {
    // Todos los filtros pasan por el mismo refresco para mantener eventos y estadísticas sincronizados.
    this.filters.update(current => ({ ...current, start: value }));
    this.scheduleRefresh();
  }

  setEnd(value: string): void {
    this.filters.update(current => ({ ...current, end: value }));
    this.scheduleRefresh();
  }

  setMinMag(value: string | number | null): void {
    this.filters.update(current => ({
      ...current,
      minMag: value === null ? '' : String(value)
    }));
    this.scheduleRefresh();
  }

  setCountry(value: string): void {
    this.filters.update(current => ({ ...current, countryId: value }));
    this.scheduleRefresh();
  }

  setMode(value: string): void {
    if (value !== 'live' && value !== 'replay') return;
    this.filters.update(current => ({
      ...current,
      mode: value,
      start: value === 'replay' ? REPLAY_START : '',
      end: value === 'replay' ? REPLAY_END : ''
    }));
    this.scheduleRefresh();
  }

  async loadMoreEvents(): Promise<void> {
    // Añade la página siguiente con los filtros vigentes; las estadísticas describen el conjunto completo.
    const config = this.config;
    const offset = this.nextCursor();
    if (!config || config.mode !== 'api' || offset === null || this.refreshing()) return;
    const query = this.buildQuery(this.filters());
    if (typeof query === 'string') {
      this.apiError.set(query);
      return;
    }
    clearTimeout(this.pollTimer);
    const requestId = ++this.requestId;
    const controller = new AbortController();
    this.activeRequest?.abort();
    this.activeRequest = controller;
    this.refreshing.set(true);
    this.apiError.set('');
    try {
      query.set('offset', String(offset));
      const page = await this.fetchEvents(config, query, controller.signal);
      if (requestId !== this.requestId || this.destroyed) return;
      const combined = [...this.events(), ...page.features];
      this.replaceEvents(combined);
      this.events.set(combined);
      this.nextCursor.set(page.metadata?.next_cursor ?? null);
      this.totalEvents.set(page.metadata?.total ?? combined.length);
      this.generated.set(this.formatTimestamp(page.metadata?.generated));
      this.markSuccessfulRead();
      this.appliedFilters.set(this.describeFilters(this.filters()));
      this.status.set('Página adicional cargada; las estadísticas describen todos los resultados filtrados.');
    } catch (error) {
      if (!controller.signal.aborted && requestId === this.requestId && !this.destroyed) {
        this.apiError.set(`No se pudo cargar la siguiente página: ${this.errorMessage(error)}`);
      }
    } finally {
      if (requestId === this.requestId) {
        this.refreshing.set(false);
        this.schedulePoll(config);
      }
    }
  }

  barWidth(count: number, rows: CountRow[]): number {
    const maximum = Math.max(1, ...rows.map(row => row.count));
    return Math.max(2, (count / maximum) * 100);
  }

  eventMagnitude(event: Earthquake): string {
    const value = event.properties?.mag;
    return typeof value === 'number' && Number.isFinite(value) ? value.toFixed(1) : 'ND';
  }

  magnitudeBand(event: Earthquake): MagnitudeBand {
    const value = event.properties?.mag;
    if (typeof value !== 'number' || !Number.isFinite(value)) return 'nd';
    if (value < 2) return 'low';
    if (value < 4) return 'moderate';
    if (value < 6) return 'strong';
    return 'major';
  }

  eventDepth(event: Earthquake): string {
    const value = event.properties?.depth_km ?? event.properties?.depth;
    return typeof value === 'number' && Number.isFinite(value) ? `${value.toFixed(1)} km` : 'ND';
  }

  eventCountry(event: Earthquake): string {
    return event.properties?.country_name || 'Sin país asignado';
  }

  formatPlace(value: string | null | undefined): string {
    const place = value?.trim();
    if (!place) return 'Sin descripción';

    const distance = place.match(/^(\d+(?:\.\d+)?)\s*km\s+(NNE|NE|ENE|ESE|SE|SSE|SSW|SW|WSW|WNW|NW|NNW|N|E|S|W)\s+of\s+(.+)$/i);
    if (distance) {
      const direction = distance[2].toUpperCase();
      return `${distance[1]} km al ${DIRECTIONS_ES[direction]} de ${this.translatePlaceName(distance[3])}`;
    }

    const region = place.match(/^(.+?),\s*(.+?)\s+region$/i);
    if (region) {
      const country = COUNTRY_NAMES_ES[region[2].toLowerCase()] ?? region[2];
      return `${this.translatePlaceName(region[1])}, región de ${country}`;
    }

    const islandRegion = place.match(/^(.+?)\s+Islands region$/i);
    if (islandRegion) return `Región de las islas ${islandRegion[1]}`;

    const namedRegion = place.match(/^(.+?)\s+region$/i);
    if (namedRegion) return `Región de ${this.translatePlaceName(namedRegion[1])}`;

    return this.translatePlaceName(place);
  }

  private translatePlaceName(place: string): string {
    const geographicFeature = place.match(/^(.+?)\s+(Islands|Island|Ridge|Trench|Basin)$/i);
    if (!geographicFeature) return place;

    const featureNames: Record<string, string> = {
      islands: 'Islas',
      island: 'Isla',
      ridge: 'Dorsal',
      trench: 'Fosa',
      basin: 'Cuenca'
    };
    const feature = featureNames[geographicFeature[2].toLowerCase()];
    return `${feature} ${geographicFeature[1]}`;
  }

  formatTimestamp(value: number | null | undefined): string {
    if (typeof value !== 'number' || !Number.isFinite(value)) return 'ND';
    const date = new Date(value);
    return Number.isNaN(date.getTime()) ? 'ND' : date.toISOString();
  }

  readingAge(): string {
    const readAt = this.readingTimestamp();
    if (readAt === null) return 'sin una lectura válida';
    return this.formatAge(Math.max(0, Math.floor((this.ageTick() - readAt) / 1000)));
  }

  ingestionAgeText(): string {
    const age = this.ingestionAge();
    return age === null ? 'antigüedad desconocida' : `hace ${this.formatAge(age)}`;
  }

  formatPeriod(value: string | null): string {
    if (!value) return 'ND';
    const parsed = new Date(value);
    return Number.isNaN(parsed.getTime()) ? value : parsed.toISOString();
  }

  formatHour(timestamp: number): string {
    const date = new Date(timestamp);
    return Number.isNaN(date.getTime()) ? 'ND' : `${date.toISOString().slice(0, 16).replace('T', ' ')} UTC`;
  }

  ngOnDestroy(): void {
    this.destroyed = true;
    clearTimeout(this.pollTimer);
    clearTimeout(this.debounceTimer);
    clearInterval(this.ageTimer);
    this.requestId++;
    this.activeRequest?.abort();
    this.lifetime.abort();
  }

  private async refresh(): Promise<void> {
    const config = this.config;
    if (!config || this.destroyed) return;
    clearTimeout(this.pollTimer);
    const requestId = ++this.requestId;
    const controller = new AbortController();
    this.activeRequest?.abort();
    this.activeRequest = controller;
    this.refreshing.set(true);
    this.apiError.set('');
    const currentFilters = this.filters();

    try {
      const query = this.buildQuery(currentFilters);
      if (typeof query === 'string') throw new Error(query);

      if (config.mode === 'demo') {
        const data = await this.readJson<Events>(
          'datos/terremotos-demo.geojson',
          {},
          controller.signal
        );
        this.validateEvents(data);
        if (requestId !== this.requestId || this.destroyed) return;
        this.replaceEvents(data.features);
        this.events.set(data.features);
        this.statistics.set(null);
        this.totalEvents.set(data.features.length);
        this.nextCursor.set(null);
        this.generated.set(this.formatTimestamp(data.metadata?.generated));
        this.lastIngestion.set(null);
        this.ingestionAge.set(null);
        this.ingestionStale.set(false);
        this.period.set('Datos de demostración');
        this.appliedFilters.set(this.describeFilters(currentFilters));
        this.markSuccessfulRead();
        this.status.set('DEMOSTRACIÓN · datos ficticios; no representa actividad real');
        return;
      }

      const base = config.apiBaseUrl.replace(/\/+$/, '');
      // Reutiliza exactamente los mismos parámetros para que eventos y estadísticas representen el mismo corte.
      const [page, statistics, health] = await Promise.all([
        this.fetchEvents(config, query, controller.signal),
        this.readJson<ApiStatistics>(
          `${base}/api/estadisticas?${query.toString()}`,
          this.apiHeaders(),
          controller.signal
        ),
        this.readJson<ApiHealth>(
          `${base}/api/salud`,
          this.apiHeaders(),
          controller.signal
        )
      ]);
      this.validateStatistics(statistics);
      this.validateHealth(health);
      if (requestId !== this.requestId || this.destroyed) return;

      this.replaceEvents(page.features);
      this.events.set(page.features);
      this.statistics.set(statistics);
      this.nextCursor.set(page.metadata?.next_cursor ?? null);
      this.totalEvents.set(page.metadata?.total ?? page.features.length);
      this.generated.set(this.formatTimestamp(page.metadata?.generated));
      this.lastIngestion.set(health.last_ingestion ?? page.metadata?.last_ingestion ?? null);
      this.ingestionAge.set(health.ingestion_age_seconds);
      this.ingestionStale.set(health.ingestion_stale || health.last_ingestion === null);
      this.period.set(`${this.formatPeriod(statistics.metadata.start)} — ${this.formatPeriod(statistics.metadata.end)} (UTC)`);
      this.appliedFilters.set(this.describeFilters(currentFilters));
      this.markSuccessfulRead();
      this.status.set('API accesible · mapa, tabla y estadísticas actualizados con los mismos filtros.');
    } catch (error) {
      if (!controller.signal.aborted && requestId === this.requestId && !this.destroyed) {
        // No se borran los últimos datos válidos: el error se informa y se reintenta en el siguiente sondeo.
        this.apiError.set(this.errorMessage(error));
        this.status.set('Falló la actualización; se conserva en pantalla la última lectura válida.');
      }
    } finally {
      if (requestId === this.requestId) {
        this.refreshing.set(false);
        this.schedulePoll(config);
      }
    }
  }

  private scheduleRefresh(): void {
    // Agrupa cambios rápidos y cancela la consulta anterior para evitar resultados de filtros obsoletos.
    const config = this.config;
    if (!config || this.destroyed) return;
    clearTimeout(this.pollTimer);
    clearTimeout(this.debounceTimer);
    this.requestId++;
    this.activeRequest?.abort();
    this.refreshing.set(false);
    this.debounceTimer = setTimeout(() => void this.refresh(), 250);
  }

  private schedulePoll(config: Config): void {
    clearTimeout(this.pollTimer);
    if (this.destroyed || config.mode !== 'api') return;
    this.pollTimer = setTimeout(
      () => void this.refresh(),
      Math.max(10000, Number(config.refreshMs) || 60000)
    );
  }

  private async fetchEvents(
    config: Config,
    query: URLSearchParams,
    signal: AbortSignal
  ): Promise<Events> {
    // El mismo URLSearchParams se comparte con /api/estadisticas desde refresh().
    const url = `${config.apiBaseUrl.replace(/\/+$/, '')}/api/eventos?${query.toString()}`;
    const data = await this.readJson<Events>(url, this.apiHeaders(), signal);
    this.validateEvents(data);
    return data;
  }

  private buildQuery(filters: Filters): URLSearchParams | string {
    const startResult = this.toUtcTimestamp(filters.start, 'inicio');
    if (startResult.error) return startResult.error;
    const endResult = this.toUtcTimestamp(filters.end, 'fin');
    if (endResult.error) return endResult.error;
    const start = startResult.value;
    const end = endResult.value;
    if (start && end && Date.parse(end) <= Date.parse(start)) {
      return 'El fin del periodo debe ser posterior al inicio.';
    }
    const minMag = filters.minMag.trim();
    if (minMag && !Number.isFinite(Number(minMag))) {
      return 'La magnitud mínima debe ser un número finito.';
    }
    const query = new URLSearchParams({ mode: filters.mode });
    if (start) query.set('start', start);
    if (end) query.set('end', end);
    if (minMag) query.set('min_mag', minMag);
    if (filters.countryId) query.set('country_id', filters.countryId);
    return query;
  }

  private toUtcTimestamp(value: string, label: string): { value: string | null; error: string | null } {
    if (!value.trim()) return { value: null, error: null };
    const timestamp = Date.parse(`${value}Z`);
    if (!Number.isFinite(timestamp)) {
      return { value: null, error: `La fecha/hora de ${label} no es válida.` };
    }
    return { value: new Date(timestamp).toISOString(), error: null };
  }

  private async readJson<T>(
    url: string,
    headers: Record<string, string>,
    signal: AbortSignal
  ): Promise<T> {
    // Expone errores HTTP/JSON y aplica un límite de espera sin ocultar fallos del servidor.
    const response = await fetch(url, {
      headers,
      cache: 'no-store',
      signal: AbortSignal.any([signal, AbortSignal.timeout(15000)])
    });
    const body = await response.text();
    let payload: unknown;
    try {
      payload = body ? JSON.parse(body) : null;
    } catch {
      throw new Error('La respuesta no es JSON; comprueba la URL de la API y el túnel ngrok.');
    }
    if (!response.ok) {
      const detail = payload && typeof payload === 'object' && 'detail' in payload
        ? String(payload.detail)
        : `HTTP ${response.status}`;
      throw new Error(detail);
    }
    return payload as T;
  }

  private apiHeaders(): Record<string, string> {
    return { 'ngrok-skip-browser-warning': 'true' };
  }

  private validateConfig(config: Config): void {
    if (!config || !['api', 'demo'].includes(config.mode)) {
      throw new Error('mode debe ser "api" o "demo".');
    }
    if (typeof config.apiBaseUrl !== 'string' || !config.apiBaseUrl.trim()) {
      throw new Error('apiBaseUrl debe indicar la URL del servidor.');
    }
    if (!Number.isFinite(config.refreshMs) || config.refreshMs <= 0) {
      throw new Error('refreshMs debe ser un número positivo.');
    }
  }

  private validateEvents(data: Events): void {
    if (!data || data.type !== 'FeatureCollection' || !Array.isArray(data.features)) {
      throw new Error('La API no devolvió un FeatureCollection válido.');
    }
    if (data.metadata?.truncated && typeof data.metadata.next_cursor !== 'number') {
      throw new Error('La API indicó resultados truncados sin un cursor de continuación.');
    }
  }

  private validateStatistics(data: ApiStatistics): void {
    if (!data?.metadata || !Array.isArray(data.events_by_country)
      || !Array.isArray(data.events_by_hour)
      || !Array.isArray(data.magnitude_distribution?.bins)
      || !Array.isArray(data.depth_distribution?.bins)
      || !data.historical_comparison) {
      throw new Error('La API no devolvió una respuesta de estadísticas completa y válida.');
    }
  }

  private validateHealth(data: ApiHealth): void {
    if (!data || typeof data.status !== 'string'
      || typeof data.ingestion_stale !== 'boolean') {
      throw new Error('La API no devolvió un estado de salud válido.');
    }
  }

  private replaceEvents(features: Earthquake[]): void {
    // Sustituye la capa completa antes de añadir los puntos, evitando duplicados tras cada actualización.
    const next = L.layerGroup();
    let omitted = 0;
    for (const feature of features) {
      const properties = feature.properties;
      const coordinates = feature.geometry?.type === 'Point' ? feature.geometry.coordinates : [];
      const [longitude, latitude] = coordinates;
      if (!properties || !Number.isFinite(longitude) || !Number.isFinite(latitude)
        || Math.abs(longitude) > 180 || Math.abs(latitude) > 90) {
        omitted++;
        continue;
      }
      const magnitude = typeof properties.mag === 'number' && Number.isFinite(properties.mag)
        ? properties.mag
        : null;
      const rawDepth = properties.depth_km ?? properties.depth;
      const depth = typeof rawDepth === 'number' && Number.isFinite(rawDepth) ? rawDepth : null;
      const color = depth === null
        ? '#64748b'
        : depth < 70 ? '#d99b00' : depth < 300 ? '#e35b37' : '#7b3fb6';
      const popup = document.createElement('article');
      popup.className = 'quake-popup';
      const heading = document.createElement('strong');
      heading.className = 'quake-popup-title';
      heading.textContent = this.formatPlace(properties.place);
      popup.append(heading);
      const details = document.createElement('dl');
      const rows: [string, string][] = [
        ['Magnitud', magnitude === null ? 'ND' : magnitude.toFixed(1)],
        ['Profundidad', depth === null ? 'ND' : `${depth.toFixed(1)} km`],
        ['Hora UTC', this.formatTimestamp(properties.time)],
        ['País', properties.country_name ?? 'Sin país asignado'],
        ['Coordenadas', `${latitude.toFixed(4)}, ${longitude.toFixed(4)}`],
        ['ID', String(feature.id ?? 'ND')]
      ];
      for (const [label, value] of rows) {
        const row = document.createElement('div');
        const term = document.createElement('dt');
        const description = document.createElement('dd');
        term.textContent = label;
        description.textContent = value;
        row.append(term, description);
        details.append(row);
      }
      popup.append(details);
      L.circleMarker([latitude, longitude], {
        radius: magnitude === null ? 4 : Math.max(3, Math.min(18, 3 + (magnitude + 1) * 2)),
        color,
        fillColor: color,
        fillOpacity: 0.78,
        weight: 1
      }).bindPopup(popup).addTo(next);
    }
    this.eventsLayer.clearLayers();
    this.eventsLayer.addLayer(next);
    this.omitted.set(omitted);
  }

  private getCountryOptions(collection: FeatureCollection): CountryOption[] {
    const countries = new Map<string, string>();
    for (const feature of collection.features) {
      const id = feature.properties?.ADM0_A3;
      const name = feature.properties?.NAME_ES ?? feature.properties?.ADMIN;
      if (typeof id === 'string' && typeof name === 'string') countries.set(id, name);
    }
    return [...countries.entries()]
      .map(([id, name]) => ({ id, name }))
      .sort((a, b) => a.name.localeCompare(b.name, 'es'));
  }

  private describeFilters(filters: Filters): string {
    const period = filters.start || filters.end
      ? `${filters.start || 'sin inicio'} — ${filters.end || 'sin fin'} UTC`
      : 'últimas 24 h predeterminadas por la API';
    const magnitude = filters.minMag.trim() ? ` · magnitud ≥ ${filters.minMag}` : '';
    const country = filters.countryId === 'unassigned'
      ? ' · Sin país asignado'
      : filters.countryId
        ? ` · país ${this.countryOptions().find(option => option.id === filters.countryId)?.name ?? filters.countryId}`
        : ' · todos los países';
    return `${filters.mode === 'replay' ? 'reproducción histórica' : 'tiempo real'} · ${period}${magnitude}${country}`;
  }

  private markSuccessfulRead(): void {
    const now = Date.now();
    this.readingTimestamp.set(now);
    this.ageTick.set(now);
    this.lastRead.set(new Date(now).toISOString());
  }

  private formatAge(seconds: number): string {
    if (seconds < 60) return `${seconds} s`;
    if (seconds < 3600) return `${Math.floor(seconds / 60)} min`;
    if (seconds < 86400) return `${Math.floor(seconds / 3600)} h ${Math.floor((seconds % 3600) / 60)} min`;
    return `${Math.floor(seconds / 86400)} d ${Math.floor((seconds % 86400) / 3600)} h`;
  }

  private errorMessage(error: unknown): string {
    if (error instanceof DOMException && error.name === 'AbortError') return 'La consulta fue cancelada.';
    if (error instanceof Error && error.name === 'TimeoutError') return 'Tiempo de espera agotado (15 s).';
    if (error instanceof TypeError) {
      return `Falló la petición HTTP (${error.message}); comprueba que el servidor, CORS y el túnel estén disponibles.`;
    }
    return error instanceof Error ? error.message : 'Error desconocido al consultar el servidor.';
  }
}
