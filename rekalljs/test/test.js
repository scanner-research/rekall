"use strict";

import {Bounds, BoundingBox, Domain_Video, Interval} from '../dist/bundle';
import assert from 'assert';

describe('Bounds', () => {
  it("builds", () => {
    let i = new Bounds(0, 1, new BoundingBox(0, 1, 0, 1), new Domain_Video(0));
    assert.equal(i.t1, 0, 't1');
    assert.equal(i.t2, 1, 't2');
    assert.equal(i.bbox.x1, 0, 'x1');
    assert.equal(i.bbox.x2, 1, 'x2');
    assert.equal(i.bbox.y1, 0, 'y1');
    assert.equal(i.bbox.y2, 1, 'y2');
    assert.equal(i.domain.video_id, 0, 'domain');
  });

  describe('time_overlaps', () => {
    it("nonoverlapping", () => {
      let i1 = new Bounds(0, 1);
      let i2 = new Bounds(2, 3);
      assert.ok(!i1.time_overlaps(i2));
    });

    it("overlapping", () => {
      let i1 = new Bounds(0, 1);
      let i2 = new Bounds(0.5, 2);
      assert.ok(i1.time_overlaps(i2));
    });
  });
});

describe('Interval', () => {
  it('builds', () => {
    let i = new Interval(new Bounds(0), 1);
  });
});
