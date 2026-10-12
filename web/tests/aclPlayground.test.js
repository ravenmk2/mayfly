import { describe, it, expect } from 'vitest';
import { matchSegment, topicEval, evalRules, playgroundRules } from '../src/utils/aclPlayground.js';

describe('matchSegment', () => {
  it('matches + against any single level', () => {
    expect(matchSegment('+', 'room1', '', '')).toBe(true);
  });
  it('resolves {user} and {clientid} placeholders', () => {
    expect(matchSegment('{user}', 'sensor1', 'sensor1', 'cid')).toBe(true);
    expect(matchSegment('{user}', 'other', 'sensor1', 'cid')).toBe(false);
    expect(matchSegment('{user}', '', '', 'cid')).toBe(false);
    expect(matchSegment('{clientid}', 'dev-1', 'u', 'dev-1')).toBe(true);
  });
  it('compares literal segments', () => {
    expect(matchSegment('a', 'a', '', '')).toBe(true);
    expect(matchSegment('a', 'b', '', '')).toBe(false);
  });
});

describe('topicEval', () => {
  it('# matches the remaining levels', () => {
    const r = topicEval('factory/#', 'factory/a/b/c', '', '');
    expect(r.match).toBe(true);
    expect(r.segs.at(-1).label).toContain('#');
  });
  it('fails when the topic is shorter than the filter', () => {
    const r = topicEval('a/b/c', 'a/b', '', '');
    expect(r.match).toBe(false);
    expect(r.segs.at(-1).ok).toBe(false);
  });
  it('fails when the topic has extra levels without #', () => {
    const r = topicEval('a/b', 'a/b/c', '', '');
    expect(r.match).toBe(false);
  });
  it('records per-segment trace up to the first mismatch', () => {
    const r = topicEval('up/{user}', 'down/sensor1', 'sensor1', '');
    expect(r.match).toBe(false);
    expect(r.segs).toHaveLength(1);
    expect(r.segs[0].label).toBe('up ≠ down');
  });
});

describe('evalRules chain semantics', () => {
  const input = { username: 'sensor1', clientid: 'sensor-hub-01', op: 'publish', topic: 'up/sensor1' };

  it('first hit wins and stops the chain', () => {
    const { rows, hit, decision } = evalRules(playgroundRules, input);
    expect(hit.seq).toBe(3);
    expect(decision).toBe('allow');
    // 命中后不再评估后续规则
    expect(rows).toHaveLength(3);
    expect(rows[2].hit).toBe(true);
  });

  it('skips by subject, permission and topic with reasons', () => {
    const { rows } = evalRules(playgroundRules, input);
    expect(rows[0].skip).toContain('主体不匹配');
    expect(rows[1].skip).toContain('topic 不匹配');
    expect(rows[0].segs).toBeNull();
  });

  it('denies by default when nothing matches', () => {
    const { hit, decision } = evalRules(playgroundRules, {
      username: 'nobody', clientid: 'x', op: 'subscribe', topic: 'private/a',
    });
    expect(hit).toBeNull();
    expect(decision).toBe('deny');
  });

  it('deny rule hit yields deny decision', () => {
    const { hit, decision } = evalRules(playgroundRules, {
      username: 'sensor1', clientid: 'x', op: 'subscribe', topic: '$SYS/broker/uptime',
    });
    expect(hit.seq).toBe(2);
    expect(decision).toBe('deny');
  });

  it('permission mismatch is reported before topic evaluation', () => {
    const { rows } = evalRules(playgroundRules, {
      username: 'sensor1', clientid: 'x', op: 'publish', topic: 'down/sensor1',
    });
    const r4 = rows.find((r) => r.seq === 4);
    expect(r4.skip).toContain('权限不匹配');
  });
});
