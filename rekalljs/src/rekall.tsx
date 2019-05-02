import {observable, IObservableArray, toJS} from 'mobx';

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

  static from_json(obj: any): Bounds {
    return new Bounds(obj.t1, obj.t2, new BoundingBox(obj.x1, obj.x2, obj.y1, obj.y2), obj.domain);
  }
}

export class Interval<T> {
  bounds: Bounds;
  data: T;

  constructor(bounds: Bounds, data: T) {
    this.bounds = bounds;
    this.data = data;
  }

  static from_json<S>(obj: any, payload_from_json: (o: any) => S): Interval<S> {
    return new Interval(Bounds.from_json(obj.bounds), payload_from_json(obj.payload));
  }
}

export class IntervalSet<T> {
  private intervals: IObservableArray<Interval<T>>;

  constructor(intervals: Interval<T>[]) {
    this.intervals = observable.array(intervals);
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

  length(): number {
    return this.intervals.length;
  }

  arbitrary_interval(): Interval<T> | null {
    return this.length() > 0 ? this.intervals[0] : null;
  }

  add(interval: Interval<T>) {
    // TODO: should be time-sorted
    this.intervals.push(interval);
  }

  remove(interval: Interval<T>) {
    this.intervals.splice(this.intervals.indexOf(interval), 1);
  }

  static from_json<S>(obj: any, payload_from_json: (o: any) => S): IntervalSet<S> {
    return new IntervalSet(obj.map((intvl: any) => Interval.from_json(intvl, payload_from_json)));
  }

  to_json(): any {
    return toJS(this.intervals);
  }
}
