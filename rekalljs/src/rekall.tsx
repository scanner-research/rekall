export abstract class Domain {}

export class Domain_Video extends Domain {
  video_id: number;

  constructor(video_id: number) {
    super();
    this.video_id = video_id;
  }
}

export class BoundingBox {
  x1: number;
  x2: number;
  y1: number;
  y2: number;

  constructor(x1?: number, x2?: number, y1?: number, y2?: number) {
    this.x1 = x1 ? x1 : 0;
    this.x2 = x2 ? x2 : 1;
    this.y1 = y1 ? y1 : 0;
    this.y2 = y2 ? y2 : 1;
  }
}

export class Bounds {
  domain?: Domain;
  t1: number;
  t2: number;
  bbox: BoundingBox;

  constructor(t1: number, t2?: number, bbox?: BoundingBox, domain?: Domain) {
    this.t1 = t1;
    this.t2 = t2 ? t2 : t1;
    this.bbox = bbox ? bbox : new BoundingBox();
    this.domain = domain;
  }

  time_overlaps(other: Bounds): boolean {
    return this.t1 <= other.t2 && this.t2 >= other.t1;
  }
}

export class Interval<T> {
  bounds: Bounds;
  data: T;

  constructor(bounds: Bounds, data: T) {
    this.bounds = bounds;
    this.data = data;
  }
}

export class IntervalSet<T> {
  private intervals: Interval<T>[];
  dirty: boolean

  constructor(intervals: Interval<T>[]) {
    this.intervals = intervals;
    this.dirty = false;
  }

  time_overlaps(bounds: Bounds): IntervalSet<T> {
    return new IntervalSet(this.intervals.filter((i) => i.bounds.time_overlaps(bounds)));
  }

  union(other: IntervalSet<T>): IntervalSet<T> {
    return new IntervalSet(this.intervals.concat(other.intervals));
  }

  to_list(): Interval<T>[] {
    return this.intervals;
  }

  static from_json<S>(obj: any): IntervalSet<S> {
    throw new Error("Not yet implemented");
  }
}
